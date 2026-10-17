//! The answer tables (design: Answers), one decide function each: [`decide_append`] for the
//! in-flight append, [`decide_claim`] for the in-flight claim. Both read the producer answer first
//! (`Producer-Outcome` before `Producer-Epoch`), then the status of an answer without one, and
//! share [`decide_status`] for the latter. [`decide_read`] and [`decide_head`] read the answers to
//! the reconcile's reads, the `synced()` probe and the HEAD that a `412`, `404` or untyped `413`
//! sends.
//!
//! A read's `410` and `416` name the token of the stream that refused them, and come before the
//! `412` (extensions: Snapshot Metadata on HEAD and Stream Incarnation). [`decide_gone`] and
//! [`decide_past_tail`] decide from that token where a HEAD would only have read it: our token
//! settles the answer with no HEAD. Another token still sends one, for what that history inherited
//! ([`inherits`]), and so does a refusal that names none.
//!
//! A write's answer is classified once, by `ursula_headers::classify` ([`Answer::outcome`]): it
//! tells a producer answer from any other ([`producer_answer`]), and a refusal from an unknown
//! outcome. [`decide_status`] maps that outcome to the VFS's action, and where the action is not
//! the one the outcome asks of a client, the mapping says why, in one place:
//!
//! | outcome | VFS action |
//! | --- | --- |
//! | `Applied` or `Repeat` without a producer answer | blocked `rejected`: the replicator writes keyed only, and every other write reads its success in its own table |
//! | `Redirected` | resent a few times, then blocked `redirected` |
//! | `Unknown`, a `408` or a `5xx` (or no answer at all) | resend identically |
//! | `Unknown`, a `3xx` but `307`/`308` | as a redirect: never followed, the identical request goes again |
//! | `Unknown`, otherwise (a `1xx`, a `2xx` without its typed answer) | blocked `rejected`: no answer of Ursula's, and blocked resends every 30 s |
//! | `Refused`, typed `409 Stream-Closed` | reset `stream_state` |
//! | `Refused`, typed `412`, a `404` or a `416` | HEAD without a precondition |
//! | `Refused`, a `413` | a named limit lowers `L`, an untyped one sends a HEAD |
//! | `Refused`, a `401` | reread the token and resend |
//! | `Refused`, a `402` or a `403` | blocked `payment_required` or `forbidden` |
//! | `Refused`, a `408`, `429` or `5xx` | resend identically: a refusal of load or routing passes, and a keyed write's repeat applies once or gets its receipt |
//! | `Refused`, otherwise | blocked `rejected` |
//!
//! A producer answer (a receipt, `receipt-evicted`, or a producer refusal) decides by its own
//! rows in [`decide_append`] and [`decide_claim`], with its epoch, seq and receipt. A malformed
//! typed header is a protocol violation, blocked `rejected`, whatever the outcome.
//!
//! `stale-epoch` answers an append whose epoch a newer claim superseded, so it fences, and a claim
//! that a newer epoch outranks, so the claim is lost.
//!
//! [`inherits`] is the test (design: History; extensions: Snapshot Metadata on HEAD and Stream
//! Incarnation) that both a HEAD while bound and the open's table apply: a generation keeps itself
//! across a history whose `Stream-Previous-Incarnation` is its own token and whose
//! `Stream-Inherited-Offset` is at or above its acknowledged offset.

use std::time::Duration;

use ursula_headers::ClosedEpoch;
use ursula_headers::ClusterId;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;
use ursula_headers::ProducerAnswer;
use ursula_headers::Receipt;
use ursula_headers::StreamHead;
use ursula_headers::WriteOutcome;

use super::BlockedReason;
use super::ResetKind;
use super::batch::Sealed;
use crate::wire::Answer;
use crate::wire::Exchange;
use crate::wire::Typed;

/// Why a HEAD is sent before deciding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeadReason {
    /// A `412`, `404` or `416`: another history, a deleted stream, or misrouting.
    Precondition,
    /// A `413` that names no limit: a layer that does not implement the size refusals
    /// (extensions: Server Metadata).
    TooLarge,
    /// [`super::backoff::LOST_BEFORE_HEAD`] lost answers in a row for a large body: a layer in
    /// front of the server may have refused it unread.
    Unanswered,
    /// A `410` on a read that named no token, or another one ([`decide_gone`]): retention passed
    /// our tail, or the read went to another history (a `410` comes before a `412`). With our
    /// token unchanged the binding resets this kind: `fenced` for a `synced()` probe (another
    /// owner moved on), `stale` for a provisional pre-check.
    Gone(ResetKind),
}

/// What any answer without a producer outcome decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Common {
    /// Resend the identical request after backoff, never before `retry_after`; on a fresh
    /// connection when `fresh`.
    Resend {
        retry_after: Option<Duration>,
        fresh: bool,
    },
    /// HEAD without a precondition, then decide (design: History).
    Head(HeadReason),
    /// `401`: read the token again and resend; refused twice, blocked `unauthorized`.
    Reauthorize,
    /// A 3xx: resend a few times, then blocked `redirected`.
    Redirect {
        location: Option<String>,
    },
    /// A typed `413`: the named limit lowers `L`. A body above it is sealed again, smaller, under
    /// a new epoch; otherwise blocked `request_too_large`.
    LowerLimit {
        bytes: u64,
    },
    Blocked(BlockedReason),
    Reset(ResetKind),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AppendDecision {
    /// The batch landed at the expected tail; the next write starts at `next`. `delivery` says
    /// whether this attempt applied it (`200`) or an earlier one did (`204`).
    Acknowledge {
        next: Offset,
        delivery: Delivery,
    },
    /// The server forgot the epoch: claim exactly own + 1 (design: Claims).
    ContinuationClaim,
    Common(Common),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimDecision {
    /// The claim holds its epoch. `closed` is the epoch it closed, for the reconcile's
    /// cross-check.
    Won {
        receipt: Receipt,
        closed: Option<ClosedEpoch>,
        delivery: Delivery,
    },
    /// Another claimant holds the epoch, a newer one exists, or it is at or below the floor.
    /// `epoch` is the epoch the answer names, to go above.
    Lost {
        epoch: Option<Epoch>,
    },
    Common(Common),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadDecision {
    /// The body holds the bytes up to `next`.
    Bytes {
        next: Offset,
    },
    /// `410`: below retention, of the stream whose token the refusal names (`None` when it names
    /// none).
    Gone {
        incarnation: Option<Incarnation>,
    },
    Common(Common),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeadDecision {
    /// The stream still has our token; `max_request_bytes` is its advertised limit.
    Same {
        max_request_bytes: u64,
    },
    /// Another token, and the history it names inherits every byte this generation acknowledged
    /// ([`inherits`]): re-key to `incarnation` (design: History).
    ReKey {
        incarnation: Incarnation,
        from: Incarnation,
        max_request_bytes: u64,
    },
    /// Another token whose history does not inherit what this generation acknowledged: a reset
    /// `rewound`.
    Rewound,
    /// No stream at the path, on our cluster.
    Deleted,
    /// Another `Ursula-Cluster-Id`.
    WrongCluster,
    Common(Common),
}

/// What a HEAD is checked against: the generation's cluster, its token, the token it re-keyed
/// from while no write of it landed under the new one, and its acknowledged offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeadCheck<'a> {
    pub(crate) cluster_id: &'a ClusterId,
    pub(crate) incarnation: Incarnation,
    /// The token this generation re-keyed from, until a claim of it applies under the new one. Two
    /// history events with no write between them name the token from before both
    /// (extensions: Snapshot Metadata on HEAD and Stream Incarnation), so a re-key whose claim has
    /// not landed still inherits from it.
    pub(crate) previous: Option<Incarnation>,
    pub(crate) acked_offset: Offset,
}

/// Whether the history `head` names inherits a generation of token `ours` that acknowledged
/// everything below `acked_offset` (design: History;
/// extensions: Snapshot Metadata on HEAD and Stream Incarnation): the stream was last written under
/// `ours` before the history event, and its bytes survived at least up to `acked_offset`. Offsets
/// lie in one space across history events, so they compare by order.
pub(crate) fn inherits(head: &StreamHead, ours: Incarnation, acked_offset: Offset) -> bool {
    head.incarnation() != ours
        && head
            .inherited()
            .is_some_and(|i| i.previous_incarnation == ours && acked_offset <= i.offset)
}

/// Whether a log holds commits of its own after its latest ACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unacked {
    Nothing,
    /// Some, and an ACK record may trail the stream's acknowledgements of them.
    Commits,
}

/// Whether a takeover, or a provisional open before its claim applied, may re-key a generation
/// whose log holds `pending`: only with nothing pending. An ACK record trails the stream's
/// acknowledgements, so a pending tail may hold acknowledged commits a history change lost, and a
/// re-keyed log names the new token: a later open would resume it and discard that tail with no
/// salvage. The open salvages such a generation instead. A takeover or a provisional binding
/// resets `rewound` before its log rolls (a provisional one has live connections, so it cannot
/// salvage under them), and the next open salvages it.
pub(crate) fn takeover_may_rekey(pending: Unacked) -> bool {
    pending == Unacked::Nothing
}

/// The epoch a producer answer names, if any.
pub(crate) fn answer_epoch(answer: &ProducerAnswer) -> Option<Epoch> {
    match answer {
        ProducerAnswer::Receipt { epoch, .. } | ProducerAnswer::ReceiptEvicted { epoch, .. } => {
            Some(epoch.epoch())
        }
        ProducerAnswer::EpochClosed(closure) => Some(closure.epoch()),
        ProducerAnswer::KeyConflict { epoch }
        | ProducerAnswer::ProducerExpired { epoch }
        | ProducerAnswer::StaleEpoch { epoch } => Some(*epoch),
        ProducerAnswer::SeqGap { .. } | ProducerAnswer::EpochNotClaimed => None,
    }
}

/// The table for the in-flight append `(E, s)`.
pub(crate) fn decide_append(sealed: &Sealed, exchange: &Exchange) -> AppendDecision {
    let answer = match exchange {
        Exchange::NoAnswer(_) => return AppendDecision::Common(no_answer()),
        Exchange::Answered(answer) => answer,
    };
    let Some(producer) = producer_answer(answer) else {
        return AppendDecision::Common(decide_status(answer));
    };
    match producer {
        ProducerAnswer::Receipt {
            receipt, delivery, ..
        } if receipt.start() == sealed.expected_start => AppendDecision::Acknowledge {
            next: receipt.next(),
            delivery: *delivery,
        },
        ProducerAnswer::Receipt { .. } => {
            AppendDecision::Common(Common::Reset(ResetKind::StreamState))
        }
        ProducerAnswer::KeyConflict { .. } | ProducerAnswer::ReceiptEvicted { .. } => {
            AppendDecision::Common(Common::Reset(ResetKind::Fenced))
        }
        ProducerAnswer::ProducerExpired { .. } => AppendDecision::ContinuationClaim,
        ProducerAnswer::EpochClosed(_) | ProducerAnswer::StaleEpoch { .. } => {
            AppendDecision::Common(Common::Reset(ResetKind::Fenced))
        }
        ProducerAnswer::SeqGap { .. } | ProducerAnswer::EpochNotClaimed => {
            AppendDecision::Common(Common::Reset(ResetKind::StreamState))
        }
    }
}

/// The table for the in-flight claim `(E, 0)`.
pub(crate) fn decide_claim(exchange: &Exchange) -> ClaimDecision {
    let answer = match exchange {
        Exchange::NoAnswer(_) => return ClaimDecision::Common(no_answer()),
        Exchange::Answered(answer) => answer,
    };
    let Some(producer) = producer_answer(answer) else {
        return ClaimDecision::Common(decide_status(answer));
    };
    match producer {
        ProducerAnswer::Receipt {
            receipt,
            epoch,
            delivery,
            ..
        } => ClaimDecision::Won {
            receipt: *receipt,
            closed: epoch.closed_epoch(),
            delivery: *delivery,
        },
        ProducerAnswer::ReceiptEvicted { .. }
        | ProducerAnswer::EpochClosed(_)
        | ProducerAnswer::KeyConflict { .. }
        | ProducerAnswer::ProducerExpired { .. }
        | ProducerAnswer::StaleEpoch { .. } => ClaimDecision::Lost {
            epoch: answer_epoch(producer),
        },
        ProducerAnswer::SeqGap { .. } | ProducerAnswer::EpochNotClaimed => {
            ClaimDecision::Common(Common::Reset(ResetKind::StreamState))
        }
    }
}

/// A read under our token `ours`: of the reconcile, the probe or the pre-check, whose `416`
/// decides by [`decide_past_tail`]. An open's catch-up reads its answers here too, but decides
/// every one but bytes and a `410` again from [`decide_status`], so its `416` sends the HEAD that
/// re-runs the open table whatever token it names.
pub(crate) fn decide_read(exchange: &Exchange, ours: Incarnation) -> ReadDecision {
    let answer = match exchange {
        Exchange::NoAnswer(_) => return ReadDecision::Common(no_answer()),
        Exchange::Answered(answer) => answer,
    };
    match &answer.typed {
        Ok(Typed::Read { next, .. }) if answer.status == 200 => ReadDecision::Bytes { next: *next },
        Ok(Typed::ReadRefused { incarnation }) if answer.status == 410 => ReadDecision::Gone {
            incarnation: *incarnation,
        },
        Ok(Typed::ReadRefused { incarnation }) if answer.status == 416 => {
            ReadDecision::Common(decide_past_tail(*incarnation, ours))
        }
        _ => ReadDecision::Common(decide_status(answer)),
    }
}

/// A `410` on the `synced()` probe or a pre-check, under our token `ours`: the refusal names our
/// token, so retention passed our tail and the binding resets `kind` with no HEAD. A token derives
/// from a history id the group draws at random, so ours names our stream and never one of another
/// deployment, which the HEAD's cluster check would catch. Another token takes the history path
/// through a HEAD, which names what that history inherited, and so does a refusal that names none.
pub(crate) fn decide_gone(
    incarnation: Option<Incarnation>,
    ours: Incarnation,
    kind: ResetKind,
) -> Common {
    match incarnation {
        Some(token) if token == ours => Common::Reset(kind),
        Some(_) | None => Common::Head(HeadReason::Gone(kind)),
    }
}

/// A `416` on a read under our token `ours`, a read past the tail. Under our token that cannot
/// happen (H1), and the precondition is checked after the range (extensions: Producer Write
/// Answers). Naming our token, it is resent after backoff, as a HEAD that found our token would
/// have it resent. Another token means the history changed, and that token or none sends the HEAD
/// of a `412`.
pub(crate) fn decide_past_tail(incarnation: Option<Incarnation>, ours: Incarnation) -> Common {
    match incarnation {
        Some(token) if token == ours => Common::Resend {
            retry_after: None,
            fresh: false,
        },
        Some(_) | None => Common::Head(HeadReason::Precondition),
    }
}

/// The HEAD after a `412`, `404` or untyped `413`: our cluster and token, or why not.
pub(crate) fn decide_head(exchange: &Exchange, check: HeadCheck<'_>) -> HeadDecision {
    let answer = match exchange {
        Exchange::NoAnswer(_) => return HeadDecision::Common(no_answer()),
        Exchange::Answered(answer) => answer,
    };
    match &answer.typed {
        Ok(Typed::Head {
            identity: Some(identity),
            stream,
        }) if matches!(answer.status, 200 | 404) => {
            if &identity.cluster_id != check.cluster_id {
                return HeadDecision::WrongCluster;
            }
            match stream {
                None => HeadDecision::Deleted,
                Some(head) if head.incarnation() == check.incarnation => HeadDecision::Same {
                    max_request_bytes: head.max_request_bytes(),
                },
                Some(head) => match [Some(check.incarnation), check.previous]
                    .into_iter()
                    .flatten()
                    .find(|ours| inherits(head, *ours, check.acked_offset))
                {
                    Some(from) => HeadDecision::ReKey {
                        incarnation: head.incarnation(),
                        from,
                        max_request_bytes: head.max_request_bytes(),
                    },
                    None => HeadDecision::Rewound,
                },
            }
        }
        Ok(Typed::Head { identity: None, .. }) if matches!(answer.status, 200 | 404) => {
            // A server without `Ursula-Protocol: 2`.
            HeadDecision::Common(Common::Blocked(BlockedReason::Rejected {
                status: answer.status,
            }))
        }
        _ => HeadDecision::Common(decide_status(answer)),
    }
}

fn no_answer() -> Common {
    Common::Resend {
        retry_after: None,
        fresh: true,
    }
}

/// The producer answer whose outcome `classify` read: a receipt or `receipt-evicted` (`Applied`,
/// `Repeat`), or a producer refusal (`Refused` with that answer's code). `None` for every other
/// answer, which [`decide_status`] decides.
///
/// A producer answer comes only to a keyed write, which `classify` reads with the same
/// `ProducerAnswer::parse`: a receipt is always `Applied` or `Repeat`, and a producer refusal
/// always `Refused` with its own code. The debug assertions hold that; a release build hands an
/// answer that broke it to [`decide_status`].
fn producer_answer(answer: &Answer) -> Option<&ProducerAnswer> {
    let Ok(Typed::Producer(producer)) = &answer.typed else {
        return None;
    };
    debug_assert!(
        answer.outcome.is_some(),
        "a producer answer to a request that is no write: {producer:?}"
    );
    let outcome = answer.outcome.as_ref()?;
    let code = producer.error_code();
    let read = match outcome {
        WriteOutcome::Applied | WriteOutcome::Repeat => {
            debug_assert!(
                code.is_none(),
                "classify read the producer refusal {producer:?} as {outcome:?}"
            );
            true
        }
        WriteOutcome::Refused(refused) => code.as_ref() == Some(refused),
        WriteOutcome::Redirected | WriteOutcome::Unknown => false,
    };
    debug_assert!(
        read,
        "classify read the producer answer {producer:?} as {outcome:?}"
    );
    read.then_some(producer)
}

/// The rows of an answer without a producer answer: a write's by its outcome ([`decide_write`]),
/// a read's or a HEAD's by its status ([`decide_unwritten`]). A malformed typed header is a
/// protocol violation whatever the outcome.
pub(crate) fn decide_status(answer: &Answer) -> Common {
    let typed = match &answer.typed {
        Err(_malformed) => return rejected(answer),
        Ok(typed) => typed,
    };
    match &answer.outcome {
        Some(outcome) => decide_write(answer, typed, outcome),
        None => decide_unwritten(answer, typed),
    }
}

/// The VFS's action for a write's outcome, as `ursula_headers::classify` read it: the module
/// doc's table.
fn decide_write(answer: &Answer, typed: &Typed, outcome: &WriteOutcome) -> Common {
    match outcome {
        // A success without a producer answer: the replicator writes keyed only, and every other
        // write reads its success in its own table, so this is no answer it waits for.
        WriteOutcome::Applied | WriteOutcome::Repeat => rejected(answer),
        WriteOutcome::Redirected => redirect(answer),
        WriteOutcome::Unknown => match answer.status {
            408 | 500..=599 => resend(answer),
            // A `3xx` but `307` or `308` proves nothing. The VFS never follows a redirect, so it
            // sends the identical request again, then blocks `redirected`, which still sends it
            // every 30 s.
            300..=399 => redirect(answer),
            // A `1xx`, or a success without the typed answer Ursula sends for the write: no
            // answer of Ursula's. Blocked `rejected` sends the identical request every 30 s.
            _ => rejected(answer),
        },
        WriteOutcome::Refused(_) => decide_refusal(answer, typed),
    }
}

/// A refusal: its typed detail first, then its status.
fn decide_refusal(answer: &Answer, typed: &Typed) -> Common {
    match typed {
        Typed::StreamClosed(_) => Common::Reset(ResetKind::StreamState),
        Typed::IncarnationMismatch(_) => Common::Head(HeadReason::Precondition),
        Typed::TooLarge(Some(limit)) => Common::LowerLimit {
            bytes: limit.bytes(),
        },
        Typed::TooLarge(None) => Common::Head(HeadReason::TooLarge),
        _ => match answer.status {
            // A `416` answers a read past the tail. Under our token that cannot happen (H1), and the
            // precondition is checked after the range (extensions: Producer Write Answers): the
            // history changed.
            404 | 416 => Common::Head(HeadReason::Precondition),
            401 => Common::Reauthorize,
            402 => Common::Blocked(BlockedReason::PaymentRequired),
            // A `403` with `Producer-Epoch` is a producer answer: this one is an authorizer's.
            403 => Common::Blocked(BlockedReason::Forbidden),
            // A refusal of load or routing (a `408` before any of the body was forwarded, a
            // `429`, a `5xx` whose code proves nothing applied) passes: the identical request
            // goes again, and a keyed write's repeat applies once or gets its receipt.
            408 | 429 | 500..=599 => resend(answer),
            // A request the server cannot take.
            _ => rejected(answer),
        },
    }
}

/// A read's or a HEAD's answer that its own table did not read. A read changes nothing, so
/// whether it applied never matters: a `3xx` is a redirect, and every other status decides as a
/// write's refusal of that status does (a `5xx` is resent).
fn decide_unwritten(answer: &Answer, typed: &Typed) -> Common {
    match answer.status {
        300..=399 => redirect(answer),
        _ => decide_refusal(answer, typed),
    }
}

fn resend(answer: &Answer) -> Common {
    Common::Resend {
        retry_after: answer.retry_after,
        fresh: false,
    }
}

fn redirect(answer: &Answer) -> Common {
    Common::Redirect {
        location: answer.location.clone(),
    }
}

fn rejected(answer: &Answer) -> Common {
    Common::Blocked(BlockedReason::Rejected {
        status: answer.status,
    })
}
