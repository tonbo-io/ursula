//! The answer tables of design §4.7, one decide function each: [`decide_append`] for the in-flight
//! append, [`decide_claim`] for the in-flight claim. Both read the producer answer first
//! (`Producer-Outcome` before `Producer-Epoch`), then the status of an answer without one, and
//! share [`decide_status`] for the latter. [`decide_read`] and [`decide_head`] read the answers to
//! the reconcile's reads, the `synced()` probe and the HEAD that a `412`, `404` or untyped `413`
//! sends.
//!
//! `stale-epoch` answers an append whose epoch a newer claim superseded, so it fences, and a claim
//! that a newer epoch outranks, so the claim is lost.

use std::time::Duration;

use ursula_headers::ClosedEpoch;
use ursula_headers::ClusterId;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;
use ursula_headers::ProducerAnswer;
use ursula_headers::Receipt;
use ursula_headers::RoutingAnswer;

use super::BlockedReason;
use super::ResetKind;
use super::batch::Sealed;
use crate::wire::Answer;
use crate::wire::Exchange;
use crate::wire::Typed;

/// Why a HEAD is sent before deciding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeadReason {
    /// A `412` or `404`: another history, a deleted stream, or misrouting.
    Precondition,
    /// A `413` that names no limit: a layer outside #581 §6.4.
    TooLarge,
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
    /// HEAD without a precondition, then decide (design §6.2).
    Head(HeadReason),
    /// `401`: read the token again and resend; refused twice, blocked `unauthorized`.
    Reauthorize,
    /// A 3xx: resend a few times, then blocked `redirected`.
    Redirect {
        location: Option<String>,
    },
    /// A typed `413`: the named limit lowers `L`. With no re-cut in M1, blocked
    /// `request_too_large`.
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
    /// The server forgot the epoch: claim exactly own + 1 (design §4.8).
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
    /// `410`: below retention.
    Gone,
    Common(Common),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeadDecision {
    /// The stream still has our token; `max_request_bytes` is its advertised limit.
    Same {
        max_request_bytes: u64,
    },
    /// Another token: in M1 (no re-key) a reset `rewound`.
    Rewound,
    /// No stream at the path, on our cluster.
    Deleted,
    /// Another `Ursula-Cluster-Id`.
    WrongCluster,
    Common(Common),
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
    let producer = match &answer.typed {
        Ok(Typed::Producer(producer)) => producer,
        _ => return AppendDecision::Common(decide_status(answer)),
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
    let producer = match &answer.typed {
        Ok(Typed::Producer(producer)) => producer,
        _ => return ClaimDecision::Common(decide_status(answer)),
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

/// A read of the reconcile or the probe.
pub(crate) fn decide_read(exchange: &Exchange) -> ReadDecision {
    let answer = match exchange {
        Exchange::NoAnswer(_) => return ReadDecision::Common(no_answer()),
        Exchange::Answered(answer) => answer,
    };
    match &answer.typed {
        Ok(Typed::Read { next, .. }) if answer.status == 200 => ReadDecision::Bytes { next: *next },
        Ok(Typed::Untyped) if answer.status == 410 => ReadDecision::Gone,
        _ => ReadDecision::Common(decide_status(answer)),
    }
}

/// The HEAD after a `412`, `404` or untyped `413`: our cluster and token, or why not.
pub(crate) fn decide_head(
    exchange: &Exchange,
    cluster_id: &ClusterId,
    incarnation: Incarnation,
) -> HeadDecision {
    let answer = match exchange {
        Exchange::NoAnswer(_) => return HeadDecision::Common(no_answer()),
        Exchange::Answered(answer) => answer,
    };
    match &answer.typed {
        Ok(Typed::Head {
            identity: Some(identity),
            stream,
        }) if matches!(answer.status, 200 | 404) => {
            if &identity.cluster_id != cluster_id {
                return HeadDecision::WrongCluster;
            }
            match stream {
                None => HeadDecision::Deleted,
                Some(head) if head.incarnation() != incarnation => HeadDecision::Rewound,
                Some(head) => HeadDecision::Same {
                    max_request_bytes: head.max_request_bytes(),
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

/// The rows of an answer without a producer outcome.
pub(crate) fn decide_status(answer: &Answer) -> Common {
    let status = answer.status;
    let typed = match &answer.typed {
        Err(_malformed) => return Common::Blocked(BlockedReason::Rejected { status }),
        Ok(typed) => typed,
    };
    match typed {
        Typed::StreamClosed(_) => Common::Reset(ResetKind::StreamState),
        Typed::IncarnationMismatch(_) => Common::Head(HeadReason::Precondition),
        Typed::TooLarge(Some(limit)) => Common::LowerLimit {
            bytes: limit.bytes(),
        },
        Typed::TooLarge(None) => Common::Head(HeadReason::TooLarge),
        Typed::Routing(RoutingAnswer::NotLeader { .. }) => Common::Redirect {
            location: answer.location.clone(),
        },
        Typed::Routing(RoutingAnswer::NoLeader { .. }) => Common::Resend {
            retry_after: answer.retry_after,
            fresh: false,
        },
        _ => match status {
            404 => Common::Head(HeadReason::Precondition),
            401 => Common::Reauthorize,
            402 => Common::Blocked(BlockedReason::PaymentRequired),
            // A `403` with `Producer-Epoch` is a producer answer: this one is an authorizer's.
            403 => Common::Blocked(BlockedReason::Forbidden),
            300..=399 => Common::Redirect {
                location: answer.location.clone(),
            },
            408 | 429 | 500..=599 => Common::Resend {
                retry_after: answer.retry_after,
                fresh: false,
            },
            // A `2xx` without the producer headers is not a receipt, and anything else is a
            // request the server cannot take.
            _ => Common::Blocked(BlockedReason::Rejected { status }),
        },
    }
}
