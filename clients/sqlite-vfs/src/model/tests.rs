//! The model through its production entry point, the in-process transport: every answer is
//! rendered with `ursula_headers` and parsed back by `wire`, as a client reads it. Rows cite
//! the producer rules (extensions: Producer Write Answers).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use ursula_headers::ClosedBy;
use ursula_headers::ClosedEpoch;
use ursula_headers::ClusterId;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::ErrorBody;
use ursula_headers::ErrorCode;
use ursula_headers::HeaderError;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationMismatch;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Inherited;
use ursula_headers::MAX_INCARNATION;
use ursula_headers::Offset;
use ursula_headers::PartId;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_headers::ProducerAnswer;
use ursula_headers::ProducerId;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::ProducerWrite;
use ursula_headers::Receipt;
use ursula_headers::Seq;
use ursula_headers::SizeLimit;
use ursula_headers::SnapshotDigest;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::SnapshotRef;
use ursula_headers::StreamClosedConflict;
use ursula_headers::StreamHead;
use ursula_headers::WriteOutcome;
use ursula_headers::names;
use ursula_headers::write_outcome::inventory;

use super::CheckpointId;
use super::HistoryEvent;
use super::Limits;
use super::MAX_REQUEST_BYTES;
use super::ModelServer;
use super::faults::Fault;
use super::faults::Scripted;
use super::producer::IDLE_RETIREMENT_MS;
use crate::codec::digest::SnapshotDigester;
use crate::codec::digest::producer_digest;
use crate::codec::frame::CONTENT_TYPE;
use crate::transport::Attempt;
use crate::transport::Transport;
use crate::transport::model::ModelTransport;
use crate::wire::Answer;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::PRODUCER_ID;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::Typed;

const PATH: &str = "/v1/stream/b/db";

pub(crate) struct World {
    pub(crate) server: Arc<Mutex<ModelServer>>,
    transport: ModelTransport,
}

impl World {
    pub(crate) fn new() -> Self {
        let server = Arc::new(Mutex::new(ModelServer::new(
            ClusterId::new("model-a").unwrap(),
            7,
        )));
        let transport = ModelTransport::new(Arc::clone(&server), PATH);
        Self { server, transport }
    }

    pub(crate) fn server(&self) -> std::sync::MutexGuard<'_, ModelServer> {
        self.server.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn exchange(&self, request: &Request) -> Exchange {
        self.transport.send(request, Attempt::default())
    }

    pub(crate) fn send(&self, request: &Request) -> Answer {
        match self.exchange(request) {
            Exchange::Answered(answer) => *answer,
            Exchange::NoAnswer(no) => panic!("no answer: {no:?}"),
        }
    }

    /// Creates the stream with the VFS's content type and returns its token.
    pub(crate) fn create(&self) -> Incarnation {
        match self.send(&create()).typed {
            Ok(Typed::Created { incarnation }) => incarnation,
            other => panic!("{other:?}"),
        }
    }

    fn producer(&self, request: &Request) -> ProducerAnswer {
        match self.send(request).typed {
            Ok(Typed::Producer(answer)) => answer,
            other => panic!("{other:?}"),
        }
    }

    fn head(&self) -> (u16, Typed) {
        let answer = self.send(&Request::new(Method::Head, Target::Stream));
        (answer.status, answer.typed.unwrap())
    }

    fn stream_head(&self) -> StreamHead {
        match self.head() {
            (
                200,
                Typed::Head {
                    stream: Some(head), ..
                },
            ) => head,
            other => panic!("{other:?}"),
        }
    }
}

pub(crate) fn create() -> Request {
    Request {
        content_type: Some(CONTENT_TYPE.to_owned()),
        ..Request::new(Method::Put, Target::Stream)
    }
}

fn epoch(e: u64) -> Epoch {
    Epoch::try_from(e).unwrap()
}

fn seq(n: u64) -> Seq {
    Seq::try_from(n).unwrap()
}

fn tokened(token: Incarnation) -> Option<IncarnationPrecondition> {
    Some(IncarnationPrecondition { incarnation: token })
}

/// A keyed write `(e, n)` of `body`, with its digest, under `precondition`.
pub(crate) fn write(
    precondition: Option<IncarnationPrecondition>,
    e: u64,
    n: u64,
    body: &[u8],
) -> Request {
    Request {
        keys: ProducerRequestHeaders {
            producer: Some(ProducerWrite {
                id: ProducerId::new(PRODUCER_ID).unwrap(),
                epoch: epoch(e),
                seq: seq(n),
                digest: Some(producer_digest(body)),
            }),
            precondition,
        },
        content_type: Some(CONTENT_TYPE.to_owned()),
        body: Arc::from(body),
        ..Request::new(Method::Post, Target::Stream)
    }
}

fn receipt_of(answer: &ProducerAnswer) -> Receipt {
    match answer {
        ProducerAnswer::Receipt { receipt, .. } => *receipt,
        other => panic!("{other:?}"),
    }
}

fn read(precondition: Option<IncarnationPrecondition>, from: Option<u64>) -> Request {
    Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition,
        },
        ..Request::read(from.map(Offset::from_u64), true, 1 << 20)
    }
}

#[test]
fn receipts_are_contiguous_and_name_the_highest_applied_seq() {
    let world = World::new();
    let token = world.create();
    let claim = world.producer(&write(tokened(token), 1, 0, b"claim"));
    let first = receipt_of(&claim);
    assert_eq!(first.start(), Offset::ZERO);
    let mut tail = first.next();
    for n in 1..=3 {
        let answer = world.producer(&write(tokened(token), 1, n, &[n as u8; 10]));
        let ProducerAnswer::Receipt {
            delivery,
            seq: highest,
            receipt,
            incarnation,
            ..
        } = answer
        else {
            panic!("{answer:?}");
        };
        assert_eq!(delivery, Delivery::Applied);
        assert_eq!(highest, seq(n));
        assert_eq!(incarnation, token);
        assert_eq!(receipt.start(), tail, "row 12 lands at the tail");
        tail = receipt.next();
    }
    assert_eq!(world.stream_head().next_offset(), tail);
}

#[test]
fn a_lost_answer_is_settled_by_the_identical_repeat() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim"));
    world.server().inject(Scripted {
        method: Some(Method::Post),
        fault: Fault::DropAnswer,
        times: 1,
    });
    let append = write(tokened(token), 1, 1, b"commit");
    assert!(matches!(
        world.exchange(&append),
        Exchange::NoAnswer(NoAnswer::Lost { .. })
    ));
    // Row 9: the repeat gets the receipt of the write that applied, and nothing applies twice.
    let repeat = world.producer(&append);
    assert!(matches!(repeat, ProducerAnswer::Receipt {
        delivery: Delivery::Repeat,
        ..
    }));
    assert_eq!(world.server().frames(PATH).unwrap(), b"claimcommit");
}

#[test]
fn a_dropped_request_applies_nothing() {
    let world = World::new();
    let token = world.create();
    world.server().inject(Scripted {
        method: None,
        fault: Fault::DropRequest,
        times: 1,
    });
    assert!(matches!(
        world.exchange(&write(tokened(token), 1, 0, b"claim")),
        Exchange::NoAnswer(NoAnswer::Lost { .. })
    ));
    assert_eq!(world.server().frames(PATH).unwrap(), b"");
    world.server().inject(Scripted {
        method: None,
        fault: Fault::Unreachable,
        times: 1,
    });
    assert!(matches!(
        world.exchange(&write(tokened(token), 1, 0, b"claim")),
        Exchange::NoAnswer(NoAnswer::NotSent { .. })
    ));
}

#[test]
fn a_claim_and_its_repeat_report_the_epoch_they_closed() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim-a"));
    world.producer(&write(tokened(token), 1, 1, b"one"));
    world.producer(&write(tokened(token), 1, 2, b"two"));
    let closed = Some(ClosedEpoch {
        epoch: epoch(1),
        seq: seq(2),
    });
    // Row 6, then row 9 with N = 0.
    let claim = write(tokened(token), 2, 0, b"claim-b");
    for delivery in [Delivery::Applied, Delivery::Repeat] {
        let ProducerAnswer::Receipt {
            delivery: got,
            epoch: current,
            ..
        } = world.producer(&claim)
        else {
            panic!();
        };
        assert_eq!(got, delivery);
        assert_eq!(current.epoch(), epoch(2));
        assert_eq!(current.closed_epoch(), closed);
    }
    // Row 4: the closed epoch's next seq never applies, and its answer says up to where it did.
    let ProducerAnswer::EpochClosed(closure) = world.producer(&write(tokened(token), 1, 3, b"x"))
    else {
        panic!();
    };
    assert_eq!(closure.by(), ClosedBy::Claim);
    assert_eq!(Some(closure.closed_epoch()), closed);
}

#[test]
fn a_second_claimant_of_an_epoch_gets_key_conflict_and_a_clone_too() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 3, 0, b"activation-a"));
    // Row 8 on the claim: another claim body under (3, 0).
    assert_eq!(
        world.producer(&write(tokened(token), 3, 0, b"activation-b")),
        ProducerAnswer::KeyConflict { epoch: epoch(3) }
    );
    world.producer(&write(tokened(token), 3, 1, b"commit-a"));
    // Row 8 on the newest write: a clone's other body under (3, 1).
    assert_eq!(
        world.producer(&write(tokened(token), 3, 1, b"commit-b")),
        ProducerAnswer::KeyConflict { epoch: epoch(3) }
    );
    // Row 10 once (3, 1) is no longer the newest write.
    world.producer(&write(tokened(token), 3, 2, b"commit-c"));
    assert!(matches!(
        world.producer(&write(tokened(token), 3, 1, b"commit-b")),
        ProducerAnswer::ReceiptEvicted { .. }
    ));
}

#[test]
fn idle_expiry_raises_the_floor() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 4, 0, b"claim"));
    world.server().advance(IDLE_RETIREMENT_MS);
    // The record retires at this write; row 1 refuses the zombie's tokened write of epoch 4.
    assert_eq!(
        world.producer(&write(tokened(token), 4, 1, b"late")),
        ProducerAnswer::ProducerExpired { epoch: epoch(4) }
    );
    assert_eq!(
        world.producer(&write(tokened(token), 4, 0, b"claim")),
        ProducerAnswer::ProducerExpired { epoch: epoch(4) }
    );
    // A continuation claim at own + 1 is above the floor (row 2).
    assert!(matches!(
        world.producer(&write(tokened(token), 5, 0, b"again")),
        ProducerAnswer::Receipt {
            delivery: Delivery::Applied,
            ..
        }
    ));
}

#[test]
fn floor_decision_the_floor_binds_only_tokened_writes() {
    // The floor decision on #592: a tokened write never opens an epoch at or below the floor,
    // and an untokened writer keeps the base rule that a forgotten producer is new.
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 5, 0, b"claim"));
    world.server().advance(IDLE_RETIREMENT_MS);
    assert_eq!(
        world.producer(&write(tokened(token), 3, 0, b"zombie")),
        ProducerAnswer::ProducerExpired { epoch: epoch(5) }
    );
    assert_eq!(
        world.producer(&write(None, 2, 3, b"no record")),
        ProducerAnswer::SeqGap {
            expected: seq(0),
            received: seq(3),
        }
    );
    // The untokened claim opens a record claimed below the floor.
    assert!(matches!(
        world.producer(&write(None, 2, 0, b"base client")),
        ProducerAnswer::Receipt {
            delivery: Delivery::Applied,
            ..
        }
    ));
    assert!(matches!(
        world.producer(&write(None, 2, 1, b"base next")),
        ProducerAnswer::Receipt {
            delivery: Delivery::Applied,
            ..
        }
    ));
    // Tokened writes on that record are expired, its repeat too, until a claim above the floor.
    for refused in [
        write(tokened(token), 2, 1, b"base next"),
        write(tokened(token), 2, 2, b"tokened next"),
        write(tokened(token), 5, 0, b"at the floor"),
    ] {
        assert_eq!(world.producer(&refused), ProducerAnswer::ProducerExpired {
            epoch: epoch(5)
        });
    }
    for applied in [
        write(tokened(token), 6, 0, b"above the floor"),
        write(tokened(token), 6, 1, b"marker cleared"),
    ] {
        assert!(matches!(
            world.producer(&applied),
            ProducerAnswer::Receipt {
                delivery: Delivery::Applied,
                ..
            }
        ));
    }
}

#[test]
fn a_below_floor_claim_closes_no_epoch() {
    // #592: the claim that lifts a record opened below the floor names no closed epoch, and a
    // resend in the replaced epoch answers stale-epoch, never epoch-closed.
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 5, 0, b"claim"));
    world.server().advance(IDLE_RETIREMENT_MS);
    // The untokened claim retires the record (floor 5) and opens one below the floor.
    world.producer(&write(None, 2, 0, b"base claim"));
    world.producer(&write(None, 2, 1, b"base next"));
    let lift = write(tokened(token), 6, 0, b"above the floor");
    for request in [&lift, &lift] {
        let ProducerAnswer::Receipt { epoch: current, .. } = world.producer(request) else {
            panic!("no receipt");
        };
        assert_eq!(current.closed_epoch(), None);
    }
    assert_eq!(
        world.producer(&write(None, 2, 2, b"resend")),
        ProducerAnswer::StaleEpoch { epoch: epoch(6) }
    );
}

#[test]
fn a_token_mismatch_answers_412_for_writes_and_reads() {
    let world = World::new();
    let token = world.create();
    let other = Incarnation::try_from(token.get() ^ 1).unwrap();
    let mismatch = Ok(Typed::IncarnationMismatch(IncarnationMismatch {
        current: Some(token),
    }));
    let append = world.send(&write(tokened(other), 1, 0, b"claim"));
    assert_eq!((append.status, append.typed), (412, mismatch.clone()));
    let read = world.send(&read(tokened(other), None));
    assert_eq!((read.status, read.typed), (412, mismatch));
    assert_eq!(world.server().frames(PATH).unwrap(), b"");
}

#[test]
fn head_carries_the_identity_on_200_and_on_404() {
    let world = World::new();
    let (status, typed) = world.head();
    assert_eq!(status, 404);
    let Typed::Head {
        identity: Some(identity),
        stream: None,
    } = typed
    else {
        panic!("{typed:?}");
    };
    assert_eq!(identity.protocol, 2);
    assert_eq!(identity.cluster_id.as_str(), "model-a");

    let token = world.create();
    let head = world.stream_head();
    assert_eq!(head.incarnation(), token);
    assert_eq!(head.next_offset(), Offset::ZERO);
    assert_eq!(head.max_request_bytes(), MAX_REQUEST_BYTES);
    assert_eq!(head.max_snapshot_bytes(), None);
    assert_eq!(head.inherited(), None);
    let answer = world.send(&Request::new(Method::Head, Target::Stream));
    assert_eq!(answer.content_type.as_deref(), Some(CONTENT_TYPE));
}

#[test]
fn tokens_lie_in_2_52_to_2_53_and_change_on_history_events() {
    let world = World::new();
    let token = world.create();
    assert!(token.get() >= 1 << 52 && token.get() <= MAX_INCARNATION);
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let after = world.server().token(PATH).unwrap();
    assert_ne!(after, token);
    assert!(after.get() >= 1 << 52 && after.get() <= MAX_INCARNATION);
}

#[test]
fn create_is_keyed_by_its_configuration() {
    let world = World::new();
    let first = world.send(&create());
    assert_eq!(first.status, 201);
    let again = world.send(&create());
    assert_eq!(again.status, 200);
    assert_eq!(again.typed, first.typed, "the same token");
    let other = Request {
        content_type: Some("application/json".to_owned()),
        ..create()
    };
    assert_eq!(world.send(&other).status, 409);
    let keyed = Request {
        keys: write(None, 1, 0, b"x").keys,
        ..create()
    };
    assert_eq!(world.send(&keyed).status, 400);
    // The key includes closure.
    let closed = Request {
        closes: true,
        ..create()
    };
    assert_eq!(world.send(&closed).status, 409);
}

#[test]
fn a_closed_create_is_another_configuration() {
    let world = World::new();
    let closed = Request {
        closes: true,
        ..create()
    };
    assert_eq!(world.send(&closed).status, 201);
    assert!(world.stream_head().closed());
    assert_eq!(world.send(&closed).status, 200);
    assert_eq!(world.send(&create()).status, 409);
}

#[test]
fn a_recreated_stream_has_a_new_token() {
    let world = World::new();
    let token = world.create();
    world.server().delete(PATH).unwrap();
    let answer = world.send(&write(tokened(token), 1, 0, b"claim"));
    assert_eq!(answer.status, 404);
    assert_ne!(world.create(), token);
}

#[test]
fn a_closed_stream_refuses_only_writes_that_would_apply() {
    // Addendum 3 item 4: the producer rows come before the closed stream.
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim"));
    let newest = write(tokened(token), 1, 1, b"commit");
    world.producer(&newest);
    world.server().close(PATH).unwrap();
    let tail = world.stream_head().next_offset();
    // A repeat of the newest write gets its receipt (row 9), even with another content type.
    assert!(matches!(world.producer(&newest), ProducerAnswer::Receipt {
        delivery: Delivery::Repeat,
        ..
    }));
    let other_type = Request {
        content_type: Some("text/plain".to_owned()),
        ..newest.clone()
    };
    assert!(matches!(
        world.producer(&other_type),
        ProducerAnswer::Receipt { .. }
    ));
    // Every producer refusal comes before 409 Stream-Closed: rows 5 and 13.
    assert_eq!(
        world.producer(&write(tokened(token), 0, 1, b"x")),
        ProducerAnswer::StaleEpoch { epoch: epoch(1) }
    );
    assert_eq!(
        world.producer(&write(tokened(token), 1, 5, b"x")),
        ProducerAnswer::SeqGap {
            expected: seq(2),
            received: seq(5),
        }
    );
    // Only a write that would apply (row 12) meets the closed stream.
    let answer = world.send(&write(tokened(token), 1, 2, b"next"));
    assert_eq!(answer.status, 409);
    assert_eq!(
        answer.typed,
        Ok(Typed::StreamClosed(StreamClosedConflict {
            final_offset: tail
        }))
    );
}

#[test]
fn idle_retirement_happens_on_a_closed_stream_too() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 5, 0, b"claim"));
    world.server().close(PATH).unwrap();
    world.server().advance(IDLE_RETIREMENT_MS);
    // The record retires at this write, which raises the floor to 5 (row 1), closed or not.
    assert_eq!(
        world.producer(&write(tokened(token), 5, 1, b"late")),
        ProducerAnswer::ProducerExpired { epoch: epoch(5) }
    );
}

/// `request` with `body` in place of its own, keeping its `Producer-Digest`.
pub(crate) fn with_body(request: &Request, body: &[u8]) -> Request {
    Request {
        body: Arc::from(body),
        ..request.clone()
    }
}

/// A keyed close-only write `(e, n)`: no body, no content type, the digest of the empty body.
pub(crate) fn close_only(precondition: Option<IncarnationPrecondition>, e: u64, n: u64) -> Request {
    Request {
        content_type: None,
        closes: true,
        ..write(precondition, e, n, b"")
    }
}

#[test]
fn addendum_4_a_digest_that_does_not_match_the_body_on_a_held_key_is_key_conflict() {
    // Rows 8 to 10 compare the `Producer-Digest` header and come before the body is checked, so
    // the answer is the same with or without the token: 403 key-conflict.
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 0, 0, b"claim"));
    world.producer(&write(tokened(token), 0, 1, b"b1"));
    let mut conflicting = write(tokened(token), 0, 1, b"another body");
    conflicting.body = Arc::from(&b"b1"[..]);
    for precondition in [tokened(token), None] {
        let request = Request {
            keys: ProducerRequestHeaders {
                precondition,
                ..conflicting.keys.clone()
            },
            ..conflicting.clone()
        };
        assert_eq!(
            world.producer(&request),
            ProducerAnswer::KeyConflict { epoch: epoch(0) },
            "{precondition:?}"
        );
    }
    // A repeat whose body disagrees with its own digest is a repeat: its body is never read.
    assert!(matches!(
        world.producer(&with_body(&write(tokened(token), 0, 1, b"b1"), b"other")),
        ProducerAnswer::Receipt {
            delivery: Delivery::Repeat,
            ..
        }
    ));
}

#[test]
fn addendum_4_a_repeat_without_a_content_type_gets_its_receipt() {
    for untokened in [false, true] {
        let world = World::new();
        let token = world.create();
        world.producer(&write(tokened(token), 1, 0, b"claim"));
        // Rows 8 to 10 answer alike with the token and without it.
        let p = if untokened { None } else { tokened(token) };
        let commit = write(p, 1, 1, b"commit");
        let applied = receipt_of(&world.producer(&commit));
        let bare = Request {
            content_type: None,
            ..commit.clone()
        };
        let answer = world.producer(&bare);
        assert!(matches!(answer, ProducerAnswer::Receipt {
            delivery: Delivery::Repeat,
            ..
        }));
        assert_eq!(receipt_of(&answer), applied);
    }
}

#[test]
fn addendum_4_an_empty_write_on_an_applied_key_gets_its_row() {
    for untokened in [false, true] {
        let world = World::new();
        let token = world.create();
        world.producer(&write(tokened(token), 1, 0, b"claim"));
        // Rows 8 to 10 answer alike with the token and without it.
        let p = if untokened { None } else { tokened(token) };
        let commit = write(p, 1, 1, b"commit");
        world.producer(&commit);
        // Row 9: the digest names the newest write, the empty body is never read.
        assert!(matches!(
            world.producer(&with_body(&commit, b"")),
            ProducerAnswer::Receipt {
                delivery: Delivery::Repeat,
                ..
            }
        ));
        // Row 10: the claim, once a later seq applied.
        assert!(matches!(
            world.producer(&with_body(&write(p, 1, 0, b"claim"), b"")),
            ProducerAnswer::ReceiptEvicted { .. }
        ));
        // Row 8: the empty body's digest under the newest write's key.
        assert_eq!(
            world.producer(&write(p, 1, 1, b"")),
            ProducerAnswer::KeyConflict { epoch: epoch(1) }
        );
        // Row 13 refuses before the content checks.
        assert_eq!(
            world.producer(&write(p, 1, 5, b"")),
            ProducerAnswer::SeqGap {
                expected: seq(2),
                received: seq(5),
            }
        );
        // Row 12 would apply: only then is the empty body refused.
        let answer = world.send(&write(p, 1, 2, b""));
        assert_eq!((answer.status, answer.typed), (400, Ok(Typed::Untyped)));
    }
}

/// Sends `refused`, the next write after a claim with its content type changed by `change`, and
/// returns its answer, after checking that nothing applied.
fn content_refusal(change: impl Fn(&mut Request)) -> Answer {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim"));
    let next = write(tokened(token), 1, 1, b"commit");
    let mut refused = next.clone();
    change(&mut refused);
    let answer = world.send(&refused);
    // Nothing applied: the same seq still applies.
    assert!(matches!(world.producer(&next), ProducerAnswer::Receipt {
        delivery: Delivery::Applied,
        ..
    }));
    answer
}

#[test]
fn addendum_4_a_missing_content_type_on_a_write_that_would_apply_is_400() {
    let answer = content_refusal(|request| request.content_type = None);
    assert_eq!((answer.status, answer.typed), (400, Ok(Typed::Untyped)));
}

#[test]
fn addendum_4_another_content_type_on_a_write_that_would_apply_is_409() {
    let answer = content_refusal(|request| request.content_type = Some("text/plain".to_owned()));
    assert_eq!((answer.status, answer.typed), (409, Ok(Typed::Untyped)));
}

#[test]
fn addendum_4_a_body_that_does_not_match_its_digest_on_a_write_that_would_apply_is_400() {
    let answer = content_refusal(|request| request.body = Arc::from(&b"another body"[..]));
    assert_eq!((answer.status, answer.typed), (400, Ok(Typed::Untyped)));
}

#[test]
fn addendum_4_an_empty_write_that_would_apply_is_400_whatever_its_content_type() {
    // Base protocol §5.2: a write without a body is never refused for its content type.
    for content_type in [None, Some("text/plain"), Some(CONTENT_TYPE)] {
        let answer = content_refusal(|request| {
            request.body = Arc::from(&b""[..]);
            request.content_type = content_type.map(str::to_owned);
        });
        assert_eq!(
            (answer.status, answer.typed),
            (400, Ok(Typed::Untyped)),
            "{content_type:?}"
        );
    }
}

#[test]
fn an_unkeyed_close_only_write_closes_and_its_repeat_gets_204() {
    // Base protocol §5.2: close-only writes are idempotent.
    let world = World::new();
    world.create();
    world.server().foreign_append(PATH, b"frame").unwrap();
    let tail = world.stream_head().next_offset();
    let close = Request {
        closes: true,
        ..Request::new(Method::Post, Target::Stream)
    };
    for _ in 0..2 {
        // The rendered answer, since a parsed unkeyed `204` keeps no headers.
        let answer = world.server().handle(PATH, &close);
        assert_eq!(answer.status, 204);
        assert_eq!(answer.headers, [
            (names::STREAM_NEXT_OFFSET, tail.to_string()),
            (names::STREAM_CLOSED, "true".to_owned()),
        ]);
        assert!(world.stream_head().closed());
    }
    // An append still meets the closed stream.
    let append = Request {
        content_type: Some(CONTENT_TYPE.to_owned()),
        body: Arc::from(&b"late"[..]),
        ..Request::new(Method::Post, Target::Stream)
    };
    assert_eq!(
        world.send(&append).typed,
        Ok(Typed::StreamClosed(StreamClosedConflict {
            final_offset: tail
        }))
    );
}

#[test]
fn addendum_4_a_keyed_close_only_write_applies_with_a_receipt_and_its_repeat_gets_204() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim"));
    let tail = world.stream_head().next_offset();
    let close = close_only(tokened(token), 1, 1);
    let answer = world.send(&close);
    assert_eq!(answer.status, 200);
    let Ok(Typed::Producer(applied)) = answer.typed else {
        panic!("{answer:?}");
    };
    let receipt = receipt_of(&applied);
    assert_eq!((receipt.start(), receipt.next()), (tail, tail));
    assert!(receipt.closes_stream());
    assert!(world.stream_head().closed());
    let repeat = world.send(&close);
    assert_eq!(repeat.status, 204);
    let Ok(Typed::Producer(repeated)) = repeat.typed else {
        panic!("{repeat:?}");
    };
    assert_eq!(receipt_of(&repeated), receipt);
    // The next seq would apply, so it meets the closed stream.
    let answer = world.send(&close_only(tokened(token), 1, 2));
    assert_eq!(
        answer.typed,
        Ok(Typed::StreamClosed(StreamClosedConflict {
            final_offset: tail
        }))
    );
}

#[test]
fn addendum_4_an_append_that_closes_is_501_only_after_the_rows() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim"));
    let commit = write(tokened(token), 1, 1, b"commit");
    world.producer(&commit);
    let closing = |request: &Request| Request {
        closes: true,
        ..request.clone()
    };
    // A repeat and a refusal are answered by their rows.
    assert!(matches!(
        world.producer(&closing(&commit)),
        ProducerAnswer::Receipt {
            delivery: Delivery::Repeat,
            ..
        }
    ));
    assert!(matches!(
        world.producer(&closing(&write(tokened(token), 1, 5, b"gap"))),
        ProducerAnswer::SeqGap { .. }
    ));
    // Even without a content type, a write that would apply gets the marker.
    let applying = Request {
        content_type: None,
        ..closing(&write(tokened(token), 1, 2, b"last"))
    };
    assert_eq!(world.send(&applying).status, 501);
    assert!(!world.stream_head().closed());
}

#[test]
fn a_foreign_append_breaks_receipt_contiguity() {
    let world = World::new();
    let token = world.create();
    let claim = receipt_of(&world.producer(&write(tokened(token), 1, 0, b"claim")));
    world.server().foreign_append(PATH, b"foreign").unwrap();
    let next = receipt_of(&world.producer(&write(tokened(token), 1, 1, b"commit")));
    assert_ne!(next.start(), claim.next());
}

#[test]
fn a_producer_digest_that_does_not_match_the_body_is_400() {
    let world = World::new();
    let token = world.create();
    let mut request = write(tokened(token), 1, 0, b"claim");
    request.body = Arc::from(b"other".to_vec());
    let answer = world.send(&request);
    assert_eq!((answer.status, answer.typed), (400, Ok(Typed::Untyped)));
}

#[test]
fn a_body_over_the_limit_gets_a_typed_413_and_a_layer_an_untyped_one() {
    let world = World::new();
    let token = world.create();
    world.server().set_limits(Limits {
        max_request_bytes: 4,
        ..Limits::default()
    });
    let answer = world.send(&write(tokened(token), 1, 0, b"too large"));
    assert_eq!(answer.status, 413);
    assert_eq!(
        answer.typed,
        Ok(Typed::TooLarge(Some(SizeLimit::Request(4))))
    );
    world.server().inject(Scripted {
        method: None,
        fault: Fault::UntypedTooLarge,
        times: 1,
    });
    let answer = world.send(&write(tokened(token), 1, 0, b"ok"));
    assert_eq!(answer.typed, Ok(Typed::TooLarge(None)));
}

#[test]
fn scripted_layers_answer_in_place_of_the_server() {
    let world = World::new();
    let token = world.create();
    for fault in [
        Fault::Status {
            status: 503,
            retry_after: Some(3),
        },
        Fault::Redirect {
            location: "http://leader/s".to_owned(),
        },
    ] {
        world.server().inject(Scripted {
            method: Some(Method::Post),
            fault,
            times: 1,
        });
    }
    let unavailable = world.send(&write(tokened(token), 1, 0, b"claim"));
    assert_eq!(unavailable.status, 503);
    assert_eq!(unavailable.typed, Ok(Typed::Untyped));
    assert_eq!(
        unavailable.retry_after,
        Some(std::time::Duration::from_secs(3))
    );
    let redirect = world.send(&write(tokened(token), 1, 0, b"claim"));
    assert_eq!(redirect.status, 307);
    assert_eq!(redirect.location.as_deref(), Some("http://leader/s"));
    assert_eq!(world.server().frames(PATH).unwrap(), b"");
}

fn publish(token: Incarnation, offset: u64, body: &[u8]) -> (Request, SnapshotDigest) {
    let mut digester = SnapshotDigester::new(CONTENT_TYPE);
    digester.update(body);
    let digest = digester.finalize();
    let request = Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        content_type: Some(CONTENT_TYPE.to_owned()),
        snapshot_digest: Some(digest),
        body: Arc::from(body),
        ..Request::new(Method::Put, Target::Snapshot {
            offset: Offset::from_u64(offset),
        })
    };
    (request, digest)
}

#[test]
fn snapshots_are_keyed_by_offset_and_digest() {
    let world = World::new();
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 64]).unwrap();
    let (first, digest) = publish(token, 32, b"image at 32");
    let published = Ok(Typed::Snapshot(SnapshotPublishAnswer::Published {
        snapshot: SnapshotRef {
            offset: Offset::from_u64(32),
            digest,
        },
        incarnation: token,
    }));
    // A publish and its repeat both answer 204 with the digest.
    for _ in 0..2 {
        let answer = world.send(&first);
        assert_eq!((answer.status, answer.typed), (204, published.clone()));
    }
    // Another digest at the same offset: 409 naming the latest.
    let latest = SnapshotRef {
        offset: Offset::from_u64(32),
        digest,
    };
    let conflict = world.send(&publish(token, 32, b"other image").0);
    assert_eq!(
        conflict.typed,
        Ok(Typed::Snapshot(SnapshotPublishAnswer::Conflict { latest }))
    );
    // A later snapshot supersedes, and the earlier publish's repeat learns it.
    assert_eq!(
        world.send(&publish(token, 64, b"image at 64").0).status,
        204
    );
    let superseded = world.send(&first);
    let Ok(Typed::Snapshot(answer)) = superseded.typed else {
        panic!("{superseded:?}");
    };
    assert!(answer.supersedes(Offset::from_u64(32)));
    // A digest that does not match the body is refused.
    let (mut bad, _) = publish(token, 64, b"image at 64");
    bad.body = Arc::from(b"tampered".to_vec());
    assert_eq!(world.send(&bad).status, 400);
    // The snapshot reads back with its headers: a range past the end is cut to it.
    let chunk = |first, last| {
        world.send(&Request::snapshot_read(
            Offset::from_u64(64),
            token,
            crate::wire::SnapshotPart::Chunk { first, last },
        ))
    };
    let read = chunk(0, 1 << 20);
    assert_eq!(read.status, 206);
    let Ok(Typed::SnapshotRead {
        read: snapshot,
        range: Some(span),
    }) = read.typed
    else {
        panic!("{read:?}");
    };
    assert!(snapshot.up_to_date);
    assert_eq!((span.first(), span.last(), span.total()), (0, 10, 11));
    assert_eq!(read.body, b"image at 64");
    // A range inside the body: those bytes alone.
    let middle = chunk(6, 7);
    assert_eq!(middle.status, 206);
    assert!(matches!(
        middle.typed,
        Ok(Typed::SnapshotRead { range: Some(span), .. }) if (span.first(), span.total()) == (6, 11)
    ));
    assert_eq!(middle.body, b"at");
    // A range that starts past the end: `416`, naming the length and the token.
    let past = chunk(11, 20);
    assert_eq!(past.status, 416);
    assert_eq!(
        past.typed,
        Ok(Typed::ReadRefused {
            incarnation: Some(token)
        })
    );
    assert_eq!(error_code(&past), ErrorCode::RangeNotSatisfiable);
    // A header read asks for the first bytes alone (`Range`), answered `206`.
    let header = world.send(&Request::snapshot_read(
        Offset::from_u64(64),
        token,
        crate::wire::SnapshotPart::Header,
    ));
    assert_eq!(header.status, 206);
    assert!(matches!(header.typed, Ok(Typed::SnapshotRead { .. })));
    assert_eq!(header.body, b"image at 64");
    // Under the scripted `ignore-range`, the whole body as `200`.
    world.server().inject(Scripted {
        method: Some(Method::Get),
        fault: Fault::IgnoreRange,
        times: 1,
    });
    let whole = chunk(6, 7);
    assert_eq!(whole.status, 200);
    assert!(matches!(
        whole.typed,
        Ok(Typed::SnapshotRead { range: None, .. })
    ));
    assert_eq!(whole.body, b"image at 64");
}

#[test]
fn retention_is_monotonic() {
    let world = World::new();
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 100]).unwrap();
    let retain = |offset: u64| {
        world.send(&Request {
            keys: ProducerRequestHeaders {
                producer: None,
                precondition: tokened(token),
            },
            ..Request::new(Method::Put, Target::Retention {
                offset: Offset::from_u64(offset),
            })
        })
    };
    let retained = |offset| {
        Ok(Typed::Retained {
            offset: Offset::from_u64(offset),
        })
    };
    // Without a snapshot, and past the latest one, a boundary is refused (extensions §2.2).
    assert_eq!(retain(60).status, 409);
    assert_eq!(world.send(&publish(token, 70, b"image").0).status, 204);
    assert_eq!(retain(71).status, 409);
    assert_eq!(retain(60).typed, retained(60));
    // A repeat, and an offset below the current one, succeed with the current one.
    assert_eq!(retain(60).typed, retained(60));
    assert_eq!(retain(10).typed, retained(60));
    assert_eq!(world.stream_head().retained_offset(), Offset::from_u64(60));
    assert_eq!(world.send(&read(tokened(token), Some(10))).status, 410);
    let tail = world.send(&read(tokened(token), Some(60)));
    assert_eq!(
        tail.typed,
        Ok(Typed::Read {
            next: Offset::from_u64(100),
            up_to_date: true,
            closed: false,
        })
    );
    assert_eq!(tail.body.len(), 40);
}

#[test]
fn w5_a_history_event_reports_the_previous_token_and_the_tail() {
    // extensions: Snapshot Metadata on HEAD and Stream Incarnation: no write since the event: the
    // previous token and the current tail.
    let world = World::new();
    let old = world.create();
    world.server().foreign_append(PATH, &[1; 30]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let head = world.stream_head();
    assert_ne!(head.incarnation(), old);
    assert_eq!(
        head.inherited(),
        Some(Inherited {
            previous_incarnation: old,
            offset: Offset::from_u64(30),
        })
    );
    // A write after the event keeps the offset at the event.
    world.server().foreign_append(PATH, &[2; 5]).unwrap();
    assert_eq!(
        world.stream_head().inherited(),
        Some(Inherited {
            previous_incarnation: old,
            offset: Offset::from_u64(30),
        })
    );
}

#[test]
fn s4_8_an_accepted_loss_inherits_the_surviving_tail_at_the_event() {
    let world = World::new();
    let old = world.create();
    world.server().foreign_append(PATH, &[1; 30]).unwrap();
    let point = world.server().checkpoint();
    world.server().foreign_append(PATH, &[2; 20]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::Restore(point))
        .unwrap();
    let head = world.stream_head();
    assert_eq!(head.next_offset(), Offset::from_u64(30));
    assert_eq!(
        head.inherited(),
        Some(Inherited {
            previous_incarnation: old,
            offset: Offset::from_u64(30),
        })
    );
    assert!(matches!(
        world
            .server()
            .history_event(HistoryEvent::Restore(CheckpointId(9))),
        Err(super::ModelError::NoCheckpoint(9))
    ));
}

#[test]
fn s4_8_two_events_without_a_write_report_the_token_before_both_and_the_unchanged_tail() {
    let world = World::new();
    let old = world.create();
    world.server().foreign_append(PATH, &[1; 8]).unwrap();
    for _ in 0..2 {
        world
            .server()
            .history_event(HistoryEvent::NewHistory)
            .unwrap();
    }
    assert_eq!(
        world.stream_head().inherited(),
        Some(Inherited {
            previous_incarnation: old,
            offset: Offset::from_u64(8),
        })
    );
}

#[test]
fn s4_8_two_events_with_a_write_between_report_the_history_between() {
    // Only one predecessor is ever reported.
    let world = World::new();
    world.create();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let middle = world.server().token(PATH).unwrap();
    world.server().foreign_append(PATH, &[1; 8]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    assert_eq!(
        world.stream_head().inherited(),
        Some(Inherited {
            previous_incarnation: middle,
            offset: Offset::from_u64(8),
        })
    );
}

#[test]
fn s4_8_row_11_closes_the_epoch_at_its_last_seq_after_the_412() {
    let world = World::new();
    let old = world.create();
    world.producer(&write(tokened(old), 1, 0, b"claim"));
    world.producer(&write(tokened(old), 1, 1, b"one"));
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let new = world.server().token(PATH).unwrap();
    // The old token gets 412 first (extensions: Snapshot Metadata on HEAD and Stream Incarnation).
    assert_eq!(world.send(&write(tokened(old), 1, 2, b"two")).status, 412);
    // Row 11 under the new token: the epoch is closed at its surviving last seq.
    let ProducerAnswer::EpochClosed(closure) = world.producer(&write(tokened(new), 1, 2, b"two"))
    else {
        panic!();
    };
    // Producer-Epoch E, Producer-Closed-Epoch E, Producer-Closed-Seq r.last.
    assert_eq!(closure.by(), ClosedBy::History);
    assert_eq!(closure.epoch(), epoch(1));
    assert_eq!(closure.closed_epoch(), ClosedEpoch {
        epoch: epoch(1),
        seq: seq(1),
    });
    // Row 9 still answers the surviving newest write, under the new token.
    let ProducerAnswer::Receipt { incarnation, .. } =
        world.producer(&write(tokened(new), 1, 1, b"one"))
    else {
        panic!();
    };
    assert_eq!(incarnation, new);
    // A claim of the next epoch applies (row 6).
    assert!(matches!(
        world.producer(&write(tokened(new), 2, 0, b"claim-2")),
        ProducerAnswer::Receipt {
            delivery: Delivery::Applied,
            ..
        }
    ));
}

#[test]
fn s4_8_a_restore_reports_the_token_last_written_under_and_the_restored_tail() {
    // The source was taken after an event with no write since: the token before that event, as
    // two events with no write between them report.
    let world = World::new();
    let before = world.create();
    world.server().foreign_append(PATH, &[1; 10]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let source = world.server().token(PATH).unwrap();
    let point = world.server().checkpoint();
    world.server().foreign_append(PATH, &[2; 20]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::Restore(point))
        .unwrap();
    let head = world.stream_head();
    assert_ne!(head.incarnation(), source);
    assert_eq!(
        head.inherited(),
        Some(Inherited {
            previous_incarnation: before,
            offset: Offset::from_u64(10),
        })
    );
}

#[test]
fn s4_8_a_restore_of_a_source_written_since_its_event_reports_the_source_token() {
    let world = World::new();
    world.create();
    world.server().foreign_append(PATH, &[1; 10]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    world.server().foreign_append(PATH, &[2; 5]).unwrap();
    let source = world.server().token(PATH).unwrap();
    let point = world.server().checkpoint();
    world.server().foreign_append(PATH, &[3; 20]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::Restore(point))
        .unwrap();
    assert_eq!(
        world.stream_head().inherited(),
        Some(Inherited {
            previous_incarnation: source,
            offset: Offset::from_u64(15),
        })
    );
}

#[test]
fn s4_8_delete_and_recreate_starts_without_inheritance() {
    let world = World::new();
    let old = world.create();
    world.server().foreign_append(PATH, &[1; 10]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    world.server().delete(PATH).unwrap();
    let new = world.create();
    assert_ne!(new, old);
    let head = world.stream_head();
    assert_eq!((head.next_offset(), head.inherited()), (Offset::ZERO, None));
}

#[test]
fn s4_8_the_snapshot_offset_is_never_above_inherited_until_a_new_publish() {
    let world = World::new();
    let token = world.create();
    world.server().foreign_append(PATH, &[1; 30]).unwrap();
    assert_eq!(world.send(&publish(token, 20, b"early").0).status, 204);
    let point = world.server().checkpoint();
    world.server().foreign_append(PATH, &[2; 20]).unwrap();
    assert_eq!(world.send(&publish(token, 50, b"late").0).status, 204);
    world
        .server()
        .history_event(HistoryEvent::Restore(point))
        .unwrap();
    let head = world.stream_head();
    let inherited = head.inherited().unwrap().offset;
    assert_eq!(inherited, Offset::from_u64(30));
    assert_eq!(
        head.snapshot().map(|s| s.offset),
        Some(Offset::from_u64(20))
    );
    // A new publish under the new token may pass it.
    world.server().foreign_append(PATH, &[3; 10]).unwrap();
    let token = world.server().token(PATH).unwrap();
    assert_eq!(world.send(&publish(token, 40, b"new").0).status, 204);
    let head = world.stream_head();
    assert_eq!(head.inherited().unwrap().offset, inherited);
    assert_eq!(
        head.snapshot().map(|s| s.offset),
        Some(Offset::from_u64(40))
    );
}

#[test]
fn s4_8_read_refusals_come_in_the_order_404_410_416_412() {
    let world = World::new();
    let other = |token: Incarnation| tokened(Incarnation::try_from(token.get() ^ 1).unwrap());
    // A missing stream's `404` names no token.
    let missing = world.send(&read(None, Some(0)));
    assert_eq!(
        (missing.status, missing.typed),
        (404, Ok(Typed::ReadRefused { incarnation: None }))
    );
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 100]).unwrap();
    assert_eq!(world.send(&publish(token, 60, b"image").0).status, 204);
    let retain = Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        ..Request::new(Method::Put, Target::Retention {
            offset: Offset::from_u64(60),
        })
    };
    assert_eq!(world.send(&retain).status, 204);
    // A wrong token below retention, past the tail, then within range. The `410` and `416` name
    // the token of the stream that refused them.
    let refused = |status: u16, incarnation: Incarnation| {
        (
            status,
            Ok(Typed::ReadRefused {
                incarnation: Some(incarnation),
            }),
        )
    };
    let gone = world.send(&read(other(token), Some(10)));
    assert_eq!((gone.status, gone.typed), refused(410, token));
    let past = world.send(&read(other(token), Some(101)));
    assert_eq!((past.status, past.typed), refused(416, token));
    assert_eq!(world.send(&read(other(token), Some(60))).status, 412);
    let past = world.send(&read(tokened(token), Some(101)));
    assert_eq!((past.status, past.typed), refused(416, token));
}

/// A snapshot read's refusals in the specification's order: `404` without the stream, `404` when the
/// snapshot is not the latest and `416` for a range past its end, both naming the token, then the
/// precondition (`412`).
#[test]
fn snapshot_read_refusals_name_the_token_before_the_412() {
    let world = World::new();
    let snapshot_read = |token, offset, first, last| {
        world.send(&Request::snapshot_read(
            Offset::from_u64(offset),
            token,
            crate::wire::SnapshotPart::Chunk { first, last },
        ))
    };
    let unknown = Incarnation::try_from(1 << 52).unwrap();
    let missing = snapshot_read(unknown, 60, 0, 1);
    assert_eq!(
        (missing.status, missing.typed),
        (404, Ok(Typed::ReadRefused { incarnation: None }))
    );
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 100]).unwrap();
    assert_eq!(world.send(&publish(token, 60, b"image").0).status, 204);
    let other = Incarnation::try_from(token.get() ^ 1).unwrap();
    let named = Ok(Typed::ReadRefused {
        incarnation: Some(token),
    });
    let not_latest = snapshot_read(other, 40, 0, 1);
    assert_eq!((not_latest.status, not_latest.typed), (404, named.clone()));
    let past = snapshot_read(other, 60, 5, 9);
    assert_eq!((past.status, past.typed), (416, named));
    assert_eq!(snapshot_read(other, 60, 0, 1).status, 412);
    assert_eq!(snapshot_read(token, 60, 0, 1).status, 206);
}

#[test]
fn a_foreign_claim_fences_the_owner() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim"));
    let raw = world
        .server()
        .foreign_claim(PATH, epoch(2), b"other owner")
        .unwrap();
    assert_eq!(raw.status, 200);
    let ProducerAnswer::EpochClosed(closure) = world.producer(&write(tokened(token), 1, 1, b"x"))
    else {
        panic!();
    };
    assert_eq!(closure.epoch(), epoch(2));
}

#[test]
fn malformed_answers_are_protocol_violations() {
    // A server answer the crate refuses reaches the caller as a typed error, never a guess.
    let mut raw = crate::wire::RawAnswer::new(403);
    raw.header("producer-epoch", "4");
    assert_eq!(
        raw.parse(&write(None, 1, 1, b"x")).typed,
        Err(HeaderError::Missing {
            name: "producer-outcome"
        })
    );
}

/// Stages `body` as a part of the snapshot at `offset`: its id, or the answer's status.
fn stage(world: &World, token: Incarnation, offset: u64, body: &[u8]) -> Result<PartId, u16> {
    let answer = world.send(&stage_request(token, offset, body));
    match answer.typed {
        Ok(Typed::PartStaged(staged)) if answer.status == 201 => Ok(staged.part),
        _ => Err(answer.status),
    }
}

/// The commit at `offset` of `parts`, whose whole body digests to `digest`.
fn commit(token: Incarnation, offset: u64, parts: &[PartId], digest: SnapshotDigest) -> Request {
    let manifest = PartsManifest::new(parts.to_vec()).unwrap();
    Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        content_type: Some(CONTENT_TYPE.to_owned()),
        parts: Some(Box::new(PartsCommit::for_manifest(&manifest, digest))),
        body: Arc::from(manifest.render().into_bytes()),
        ..Request::new(Method::Put, Target::Snapshot {
            offset: Offset::from_u64(offset),
        })
    }
}

/// `DELETE {stream}/snapshot/parts`, under `token` when one is given.
fn discard(token: Option<Incarnation>) -> Request {
    Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: token.and_then(tokened),
        },
        ..Request::new(Method::Delete, Target::StagedParts)
    }
}

/// The code of an answer's error body, as the server renders it (extensions §11).
fn error_code(answer: &Answer) -> ErrorCode {
    assert_eq!(
        answer.content_type.as_deref(),
        Some(ErrorBody::CONTENT_TYPE)
    );
    ErrorBody::parse(&answer.body).unwrap().code
}

fn digest_of(body: &[u8]) -> SnapshotDigest {
    let mut digester = SnapshotDigester::new(CONTENT_TYPE);
    digester.update(body);
    digester.finalize()
}

fn with_cold_store(world: &World) {
    let limits = world.server().limits();
    world.server().set_limits(Limits {
        max_snapshot_bytes: Some(super::MAX_SNAPSHOT_BYTES),
        ..limits
    });
}

// Staging mints a new id for every request, a repeat included; a commit assembles the parts in
// manifest order, publishes, and deletes them; its repeat answers 204 from the key with the parts
// gone.
#[test]
fn parts_stage_under_minted_ids_and_a_commit_publishes_them_in_order() {
    let world = World::new();
    with_cold_store(&world);
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 64]).unwrap();
    let a = stage(&world, token, 64, b"first ").unwrap();
    let again = stage(&world, token, 64, b"first ").unwrap();
    assert_ne!(a, again, "staging is not idempotent");
    let b = stage(&world, token, 64, b"second").unwrap();
    assert_eq!(world.server().staged_parts(), 3);
    let digest = digest_of(b"first second");
    let published = Ok(Typed::Snapshot(SnapshotPublishAnswer::Published {
        snapshot: SnapshotRef {
            offset: Offset::from_u64(64),
            digest,
        },
        incarnation: token,
    }));
    let answer = world.send(&commit(token, 64, &[a.clone(), b.clone()], digest));
    assert_eq!((answer.status, answer.typed), (204, published.clone()));
    // The commit's parts are gone; the repeat staged beside them stays for the sweep.
    assert_eq!(world.server().staged_parts(), 1);
    let repeat = world.send(&commit(token, 64, &[a, b], digest));
    assert_eq!((repeat.status, repeat.typed), (204, published));
    let read = world.send(&Request::snapshot_read(
        Offset::from_u64(64),
        token,
        crate::wire::SnapshotPart::Chunk {
            first: 0,
            last: 1 << 20,
        },
    ));
    assert_eq!(read.body, b"first second");
}

// The key comes first: a commit that a later snapshot supersedes, or whose offset holds another
// one, is answered 409 naming the latest, before any part is looked at; below retention with no
// later snapshot deciding, 410; beyond the tail, 409 naming none.
#[test]
fn a_commit_checks_its_key_before_its_parts() {
    let world = World::new();
    with_cold_store(&world);
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 100]).unwrap();
    let (later, later_digest) = publish(token, 60, b"image at 60");
    assert_eq!(world.send(&later).status, 204);
    let latest = SnapshotRef {
        offset: Offset::from_u64(60),
        digest: later_digest,
    };
    let unknown = PartId::new("never-staged.part").unwrap();
    for offset in [40, 60] {
        let answer = world.send(&commit(
            token,
            offset,
            std::slice::from_ref(&unknown),
            digest_of(b"x"),
        ));
        assert_eq!(answer.status, 409);
        assert_eq!(
            answer.typed,
            Ok(Typed::Snapshot(SnapshotPublishAnswer::Conflict { latest }))
        );
    }
    let beyond = world.send(&commit(
        token,
        500,
        std::slice::from_ref(&unknown),
        digest_of(b"x"),
    ));
    assert_eq!((beyond.status, beyond.typed), (409, Ok(Typed::Untyped)));
    // Retention past an offset that no snapshot decides: 410.
    let other = World::new();
    with_cold_store(&other);
    let token = other.create();
    other.server().foreign_append(PATH, &[0; 100]).unwrap();
    other.server().streams.get_mut(PATH).unwrap().retained = Offset::from_u64(50);
    let below = commit(token, 40, &[unknown], digest_of(b"x"));
    let raw = other.server().handle(PATH, &below);
    assert_eq!(raw.status, 410);
    // As the server's: the retained offset, in `Stream-Next-Offset`.
    assert!(
        raw.headers
            .contains(&(names::STREAM_NEXT_OFFSET, Offset::from_u64(50).to_string())),
        "{raw:?}"
    );
}

// A stream holds at most so many unused parts, and bytes of them (#622): a part past either is
// 429 naming the limit, with the token. `DELETE {stream}/snapshot/parts` under the token discards
// them (204), under another is 412, and staging goes on after it.
#[test]
fn staged_parts_are_bounded_and_discarded_together() {
    let world = World::new();
    with_cold_store(&world);
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 64]).unwrap();
    let mut limits = world.server().limits();
    limits.max_staged_parts = 2;
    limits.max_staged_bytes = 3;
    world.server().set_limits(limits);
    stage(&world, token, 64, b"a").unwrap();
    stage(&world, token, 64, b"b").unwrap();
    let part = |body: &[u8]| Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        body: Arc::from(body),
        ..Request::new(Method::Post, Target::SnapshotParts {
            offset: Offset::from_u64(64),
        })
    };
    let full = world.server().handle(PATH, &part(b"c"));
    assert_eq!(full.status, 429);
    assert!(
        full.headers
            .contains(&("ursula-max-staged-parts", "2".to_owned()))
    );
    assert!(
        full.headers
            .contains(&(names::STREAM_INCARNATION, token.to_string()))
    );
    let discard = |token| Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        ..Request::new(Method::Delete, Target::StagedParts)
    };
    let other_token = Incarnation::try_from(token.get() ^ 1).unwrap();
    assert_eq!(world.send(&discard(other_token)).status, 412);
    assert_eq!(world.server().staged_parts(), 2);
    assert_eq!(world.send(&discard(token)).status, 204);
    assert_eq!(world.server().staged_parts(), 0);
    stage(&world, token, 64, b"ab").unwrap();
    let bytes = world.server().handle(PATH, &part(b"cd"));
    assert_eq!(bytes.status, 429);
    assert!(
        bytes
            .headers
            .contains(&("ursula-max-staged-bytes", "3".to_owned()))
    );
}

// The server's 429 (#622): `Ursula-Max-Staged-Parts` or `-Bytes` set to the bound, the stream's
// current token, the code STAGED_PARTS_LIMIT and no `Retry-After`. The bound is per stream id:
// a part of an earlier incarnation counts until a part or a discard deletes it, which it does
// once that part was staged more than a minute before.
#[test]
fn a_part_past_the_staged_bound_is_429_staged_parts_limit_per_stream_id() {
    let world = World::new();
    with_cold_store(&world);
    let old = world.create();
    world.server().foreign_append(PATH, &[0; 64]).unwrap();
    let mut limits = world.server().limits();
    limits.max_staged_parts = 2;
    limits.max_staged_bytes = 3;
    world.server().set_limits(limits);
    stage(&world, old, 64, b"a").unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let token = world.server().token(PATH).unwrap();
    assert_ne!(token, old);
    stage(&world, token, 64, b"b").unwrap();
    let part = |body: &[u8]| Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        body: Arc::from(body),
        ..Request::new(Method::Post, Target::SnapshotParts {
            offset: Offset::from_u64(64),
        })
    };
    // The old incarnation's part counts: two parts held.
    let full = world.send(&part(b"c"));
    assert_eq!(full.status, 429);
    assert_eq!(full.typed, Ok(Typed::StagedPartsFull));
    assert_eq!(
        full.outcome,
        Some(WriteOutcome::Refused(ErrorCode::StagedPartsLimit))
    );
    assert_eq!(full.retry_after, None);
    assert_eq!(error_code(&full), ErrorCode::StagedPartsLimit);
    let raw = world.server().handle(PATH, &part(b"c"));
    assert!(
        raw.headers
            .contains(&(names::URSULA_MAX_STAGED_PARTS, "2".to_owned())),
        "{raw:?}"
    );
    assert!(
        raw.headers
            .contains(&(names::STREAM_INCARNATION, token.to_string())),
        "{raw:?}"
    );
    assert!(
        !raw.headers
            .iter()
            .any(|(name, _)| *name == names::URSULA_MAX_STAGED_BYTES)
    );
    assert_eq!(world.server().staged_parts(), 2);
    // A minute after staging the old part still counts; past it, the next part deletes it.
    world.server().advance(super::RETIRED_MIN_AGE_MS);
    assert_eq!(world.send(&part(b"c")).status, 429);
    world.server().advance(1);
    stage(&world, token, 64, b"c").unwrap();
    assert_eq!(world.server().staged_parts(), 2, "b and c");
    // The bytes bound: b and c hold 2 bytes, 2 more would pass 3.
    assert_eq!(world.send(&discard(Some(token))).status, 204);
    stage(&world, token, 64, b"ab").unwrap();
    let bytes = world.send(&part(b"cd"));
    assert_eq!(
        (bytes.status, bytes.typed.clone()),
        (429, Ok(Typed::StagedPartsFull))
    );
    assert_eq!(error_code(&bytes), ErrorCode::StagedPartsLimit);
    let raw = world.server().handle(PATH, &part(b"cd"));
    assert!(
        raw.headers
            .contains(&(names::URSULA_MAX_STAGED_BYTES, "3".to_owned())),
        "{raw:?}"
    );
    assert!(
        raw.headers
            .contains(&(names::STREAM_INCARNATION, token.to_string()))
    );
}

// `DELETE {stream}/snapshot/parts` as the server answers it (#622): 501 NO_COLD_STORE without a
// cold store, before the stream; 404 without the stream; 412 naming the current token under
// another; 204 with the token, discarding the current incarnation's parts and an earlier one's
// staged more than a minute before; and a commit or discard of the incarnation that runs
// meanwhile is 503 SNAPSHOT_COMMIT_IN_PROGRESS with `Retry-After`, discarding nothing.
#[test]
fn discarding_staged_parts_answers_as_the_server_does() {
    let world = World::new();
    let unsupported = world.send(&discard(None));
    assert_eq!(unsupported.status, 501);
    assert_eq!(error_code(&unsupported), ErrorCode::NoColdStore);
    assert_eq!(
        unsupported.outcome,
        Some(WriteOutcome::Refused(ErrorCode::NoColdStore))
    );
    with_cold_store(&world);
    assert_eq!(world.send(&discard(None)).status, 404);
    let old = world.create();
    world.server().foreign_append(PATH, &[0; 64]).unwrap();
    stage(&world, old, 64, b"old").unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let token = world.server().token(PATH).unwrap();
    stage(&world, token, 64, b"one").unwrap();
    stage(&world, token, 64, b"two").unwrap();
    let stale = world.server().handle(PATH, &discard(Some(old)));
    assert_eq!(stale.status, 412);
    assert!(
        stale
            .headers
            .contains(&(names::STREAM_INCARNATION, token.to_string())),
        "{stale:?}"
    );
    assert_eq!(world.server().staged_parts(), 3);
    // A commit or discard of this incarnation runs: 503, nothing discarded.
    world.server().inject(Scripted {
        method: Some(Method::Delete),
        fault: Fault::CommitRefused(super::faults::CommitRefusal::InProgress),
        times: 1,
    });
    let running = world.send(&discard(Some(token)));
    assert_eq!(running.status, 503);
    assert_eq!(running.retry_after, Some(std::time::Duration::from_secs(1)));
    assert_eq!(error_code(&running), ErrorCode::SnapshotCommitInProgress);
    assert_eq!(running.outcome, Some(WriteOutcome::Unknown));
    assert_eq!(world.server().staged_parts(), 3);
    // The current incarnation's parts go; the old one, staged under a minute ago, stays.
    let raw = world.server().handle(PATH, &discard(Some(token)));
    assert_eq!(raw.status, 204);
    assert!(
        raw.headers
            .contains(&(names::STREAM_INCARNATION, token.to_string())),
        "{raw:?}"
    );
    assert_eq!(world.server().staged_parts(), 1);
    world.server().advance(super::RETIRED_MIN_AGE_MS + 1);
    let done = world.send(&discard(None));
    assert_eq!(done.status, 204);
    assert_eq!(done.outcome, Some(WriteOutcome::Applied));
    assert_eq!(world.server().staged_parts(), 0);
    // A delete of the stream takes its parts with it.
    stage(&world, token, 64, b"x").unwrap();
    world.server().delete(PATH).unwrap();
    assert_eq!(world.server().staged_parts(), 0);
}

// After the key: an unknown or listed-twice part, or a wrong count, is 400 and the parts stay
// for a corrected commit; a snapshot above the limit is 413 naming it, the parts kept; another
// token is 412; a digest that differs is 400 and consumes them.
#[test]
fn a_commit_refuses_its_parts_as_the_server_does() {
    let world = World::new();
    with_cold_store(&world);
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 64]).unwrap();
    let a = stage(&world, token, 64, b"one").unwrap();
    let b = stage(&world, token, 64, b"two").unwrap();
    let digest = digest_of(b"onetwo");
    let unknown = PartId::new("never-staged.part").unwrap();
    assert_eq!(
        world
            .send(&commit(token, 64, &[a.clone(), unknown], digest))
            .status,
        400
    );
    // A count that does not match the manifest, and an id listed twice.
    let mut miscounted = commit(token, 64, &[a.clone(), b.clone()], digest);
    miscounted.parts = Some(Box::new(PartsCommit {
        parts: ursula_headers::PartCount::try_from(3_u32).unwrap(),
        digest,
    }));
    assert_eq!(world.send(&miscounted).status, 400);
    let mut twice = commit(token, 64, &[a.clone(), b.clone()], digest);
    twice.body = Arc::from(format!("{a}\n{a}\n").into_bytes());
    assert_eq!(world.send(&twice).status, 400);
    assert_eq!(world.server().staged_parts(), 2, "nothing copied, all kept");
    // Another token: 412, before the parts.
    let other = Incarnation::try_from(token.get() ^ 1).unwrap();
    assert_eq!(
        world
            .send(&commit(other, 64, &[a.clone(), b.clone()], digest))
            .status,
        412
    );
    // Above the snapshot limit: 413 naming it, the parts kept.
    let limits = world.server().limits();
    world.server().set_limits(Limits {
        max_snapshot_bytes: Some(5),
        ..limits
    });
    let over = world.send(&commit(token, 64, &[a.clone(), b.clone()], digest));
    assert_eq!(
        (over.status, over.typed),
        (413, Ok(Typed::TooLarge(Some(SizeLimit::Snapshot(5)))))
    );
    assert_eq!(world.server().staged_parts(), 2);
    with_cold_store(&world);
    // A digest that differs: the copy consumed the parts.
    let wrong = world.send(&commit(token, 64, &[b.clone(), a.clone()], digest));
    assert_eq!(wrong.status, 400);
    assert_eq!(world.server().staged_parts(), 0);
    assert_eq!(world.send(&commit(token, 64, &[a, b], digest)).status, 400);
}

// Without a cold store a part and a commit are 501; parts no commit used are swept after a day
// on the model's clock; Busy and InProgress are scripted 503s with `Retry-After` and the server's
// codes: one commit or discard of a stream incarnation runs at a time (#622).
#[test]
fn parts_need_a_cold_store_and_are_swept_after_a_day() {
    let world = World::new();
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 64]).unwrap();
    assert_eq!(stage(&world, token, 64, b"x"), Err(501));
    let unknown = PartId::new("never-staged.part").unwrap();
    let probe = commit(token, 64, std::slice::from_ref(&unknown), digest_of(b"x"));
    let unsupported = world.send(&probe);
    assert_eq!(unsupported.status, 501);
    assert_eq!(error_code(&unsupported), ErrorCode::NoColdStore);
    with_cold_store(&world);
    let part = stage(&world, token, 64, b"x").unwrap();
    world.server().advance(super::PART_GRACE_MS - 1);
    assert_eq!(world.server().staged_parts(), 1);
    world.server().advance(1);
    let swept = world.send(&commit(token, 64, &[part], digest_of(b"x")));
    assert_eq!(swept.status, 400, "swept");
    assert_eq!(world.server().staged_parts(), 0);
    for (refusal, code, outcome) in [
        (
            super::faults::CommitRefusal::Busy,
            ErrorCode::SnapshotCommitsBusy,
            WriteOutcome::Refused(ErrorCode::SnapshotCommitsBusy),
        ),
        (
            super::faults::CommitRefusal::InProgress,
            ErrorCode::SnapshotCommitInProgress,
            WriteOutcome::Unknown,
        ),
    ] {
        world.server().inject(Scripted {
            method: Some(Method::Put),
            fault: Fault::CommitRefused(refusal),
            times: 1,
        });
        let refused = world.send(&probe);
        assert_eq!(refused.status, 503);
        assert_eq!(refused.retry_after, Some(std::time::Duration::from_secs(1)));
        assert_eq!(error_code(&refused), code);
        assert_eq!(refused.outcome, Some(outcome));
    }
}

/// One refusal the model answers: what it is, the status and code the server gives the same case,
/// the headers it names beside the error body, and its write-outcome inventory row, if any.
struct Refusal<'a> {
    what: &'a str,
    status: u16,
    code: ErrorCode,
    headers: &'a [(&'a str, &'a str)],
    row: Option<&'a str>,
}

/// The outcome `ursula_headers::classify` reads from a refusal of `status` with `code`: `Refused`
/// for a `4xx`, and for a `5xx` whose code proves nothing applied (`NO_COLD_STORE`), `Unknown`
/// for any other `5xx` (`INTERNAL`). The two sides of the snapshotter's `transient` split.
fn refused_outcome(status: u16, code: &ErrorCode) -> WriteOutcome {
    match (status, code) {
        (500..=599, ErrorCode::Internal) => WriteOutcome::Unknown,
        _ => WriteOutcome::Refused(code.clone()),
    }
}

/// Checks `expected` on an answer as it arrived: its status, `headers` (lowercase names) and
/// body, the answer `parsed` as the client reads it for `request`. Every error answer carries
/// `Cache-Control: no-store`, the JSON content type and, but on a `HEAD`, the error body; a
/// write's reads as [`refused_outcome`]; the inventory row, which the server's own tests pin,
/// has the model's status, code and outcome.
fn check_refusal(
    expected: &Refusal<'_>,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
    request: Option<&Request>,
) {
    let what = expected.what;
    let header = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    };
    assert_eq!(status, expected.status, "{what}");
    assert_eq!(header("cache-control"), Some("no-store"), "{what}");
    assert_eq!(
        header("content-type"),
        Some(ErrorBody::CONTENT_TYPE),
        "{what}"
    );
    for (name, value) in expected.headers {
        assert_eq!(header(name), Some(*value), "{what}: {name}");
    }
    if request.is_some_and(|request| request.method == Method::Head) {
        assert!(body.is_empty(), "{what}: a HEAD answer has no body");
        return;
    }
    let parsed = ErrorBody::parse(body).unwrap_or_else(|e| panic!("{what}: {e}"));
    assert_eq!(parsed.code, expected.code, "{what}");
    assert!(!parsed.message.is_empty(), "{what}");
    let Some(request) = request else {
        return;
    };
    let answer = crate::wire::parse_answer(
        request,
        status,
        headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
        body.to_vec(),
    );
    let outcome = request
        .write_kind()
        .map(|_| refused_outcome(status, &expected.code));
    assert_eq!(answer.outcome, outcome, "{what}");
    if let Some(id) = expected.row {
        let row = inventory::row(id).unwrap_or_else(|| panic!("{what}: no row {id}"));
        assert_eq!(
            (row.status, row.code.as_ref(), Some(&row.outcome)),
            (status, Some(&expected.code), answer.outcome.as_ref()),
            "{what}: row {id}"
        );
    }
}

/// Sends `request` for the stream at `path` to the model in process, through the faults, and
/// checks its answer is `expected`.
fn assert_refused(world: &World, path: &str, request: &Request, expected: &Refusal<'_>) {
    let delivered = world.server().exchange(path, request);
    let super::Delivered::Answered { answer: raw, .. } = delivered else {
        panic!("{}: {delivered:?}", expected.what);
    };
    let headers: Vec<(String, String)> = raw
        .headers
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect();
    check_refusal(expected, raw.status, &headers, &raw.body, Some(request));
}

fn refusal<'a>(
    what: &'a str,
    status: u16,
    code: ErrorCode,
    headers: &'a [(&'a str, &'a str)],
    row: Option<&'a str>,
) -> Refusal<'a> {
    Refusal {
        what,
        status,
        code,
        headers,
        row,
    }
}

/// Every refusal the model answers in process, as a client reads it: the server's error body
/// (extensions §11) with the code the server gives the same case, the JSON content type,
/// `Cache-Control: no-store` and the headers the refusal names. A write's answer reads through
/// `ursula_headers::classify` as the server's does, and where the write-outcome inventory has the
/// case's row its status, code and outcome are the model's. The refusals of the HTTP front are in
/// `the_http_front_refuses_with_the_servers_error_body`, the scripted `503`s of a commit in
/// `parts_need_a_cold_store_and_are_swept_after_a_day`.
#[test]
fn every_refusal_carries_the_servers_error_body() {
    const MISSING: &str = "/v1/stream/b/missing";
    let world = World::new();
    let token = world.create();
    let token_text = token.to_string();
    let incarnation: &[(&str, &str)] = &[(names::STREAM_INCARNATION, token_text.as_str())];
    let offset = |n: u64| Offset::from_u64(n).to_string();
    let other = Incarnation::try_from(token.get() ^ 1).unwrap();
    let snapshot_read =
        |offset: u64, part| Request::snapshot_read(Offset::from_u64(offset), token, part);
    let header_read = || snapshot_read(0, crate::wire::SnapshotPart::Header);
    let check = |path: &str, request: &Request, expected: Refusal<'_>| {
        assert_refused(&world, path, request, &expected);
    };

    // No stream.
    let head = Request::new(Method::Head, Target::Stream);
    check(
        MISSING,
        &head,
        refusal("HEAD", 404, ErrorCode::StreamNotFound, &[], None),
    );
    let claim = write(tokened(token), 1, 0, b"claim");
    check(
        MISSING,
        &claim,
        refusal(
            "append",
            404,
            ErrorCode::StreamNotFound,
            &[],
            Some("not-found"),
        ),
    );
    check(
        MISSING,
        &read(None, None),
        refusal("read", 404, ErrorCode::StreamNotFound, &[], None),
    );
    check(
        MISSING,
        &header_read(),
        refusal("snapshot read", 404, ErrorCode::StreamNotFound, &[], None),
    );
    // Not modeled: the server deletes the stream.
    check(
        PATH,
        &Request::new(Method::Delete, Target::Stream),
        refusal("DELETE", 405, ErrorCode::MethodNotAllowed, &[], None),
    );

    // Creates.
    let keyed_create = Request {
        keys: claim.keys.clone(),
        ..create()
    };
    check(
        PATH,
        &keyed_create,
        refusal("keyed create", 400, ErrorCode::InvalidProducer, &[], None),
    );
    let other_create = Request {
        content_type: Some("text/plain".to_owned()),
        ..create()
    };
    check(
        PATH,
        &other_create,
        refusal(
            "create of another configuration",
            409,
            ErrorCode::StreamAlreadyExistsConflict,
            &[],
            None,
        ),
    );

    // Writes.
    check(
        PATH,
        &write(tokened(other), 1, 0, b"claim"),
        refusal(
            "another token",
            412,
            ErrorCode::IncarnationMismatch,
            incarnation,
            Some("incarnation-mismatch"),
        ),
    );
    world.producer(&claim);
    let next = |body: &[u8]| write(tokened(token), 1, 1, body);
    check(
        PATH,
        &write(tokened(token), 1, 5, b"gap"),
        refusal(
            "a seq gap",
            409,
            ErrorCode::ProducerSeqConflict,
            &[],
            Some("producer-seq-gap"),
        ),
    );
    check(
        PATH,
        &write(tokened(token), 1, 0, b"another claim"),
        refusal(
            "a held key",
            403,
            ErrorCode::ProducerKeyConflict,
            &[],
            Some("producer-key-conflict"),
        ),
    );
    check(
        PATH,
        &next(b""),
        refusal(
            "an empty append",
            400,
            ErrorCode::EmptyAppend,
            &[],
            Some("bad-request"),
        ),
    );
    let untyped = Request {
        content_type: None,
        ..next(b"x")
    };
    check(
        PATH,
        &untyped,
        refusal(
            "no content type",
            400,
            ErrorCode::MissingContentType,
            &[],
            None,
        ),
    );
    let text = Request {
        content_type: Some("text/plain".to_owned()),
        ..next(b"x")
    };
    let tail = offset(5);
    check(
        PATH,
        &text,
        refusal(
            "another content type",
            409,
            ErrorCode::ContentTypeMismatch,
            &[(names::STREAM_NEXT_OFFSET, tail.as_str())],
            Some("conflict"),
        ),
    );
    check(
        PATH,
        &with_body(&next(b"x"), b"y"),
        refusal(
            "a body unlike its digest",
            400,
            ErrorCode::ProducerDigestMismatch,
            &[],
            None,
        ),
    );
    let closing = Request {
        closes: true,
        ..next(b"x")
    };
    // Not modeled: the server applies an append that closes.
    check(
        PATH,
        &closing,
        refusal("an append that closes", 501, ErrorCode::Internal, &[], None),
    );

    // Reads, at the tail 5.
    check(
        PATH,
        &read(tokened(token), Some(6)),
        refusal(
            "a read past the tail",
            416,
            ErrorCode::OffsetOutOfRange,
            incarnation,
            None,
        ),
    );
    check(
        PATH,
        &read(tokened(other), None),
        refusal(
            "a read under another token",
            412,
            ErrorCode::IncarnationMismatch,
            incarnation,
            None,
        ),
    );
    check(
        PATH,
        &header_read(),
        refusal(
            "no snapshot",
            404,
            ErrorCode::SnapshotNotFound,
            incarnation,
            None,
        ),
    );

    // Snapshots and retention.
    let retain = |offset: u64| Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        ..Request::new(Method::Put, Target::Retention {
            offset: Offset::from_u64(offset),
        })
    };
    // A boundary at the retained one is a repeat, without a snapshot too.
    let repeat = world.send(&retain(0));
    assert_eq!(
        (repeat.status, repeat.typed),
        (
            204,
            Ok(Typed::Retained {
                offset: Offset::ZERO
            })
        )
    );
    check(
        PATH,
        &retain(1),
        refusal(
            "retention without a snapshot",
            409,
            ErrorCode::SnapshotConflict,
            &[(names::STREAM_NEXT_OFFSET, tail.as_str())],
            None,
        ),
    );
    check(
        PATH,
        &publish(token, 6, b"image").0,
        refusal(
            "a publish beyond the tail",
            409,
            ErrorCode::SnapshotConflict,
            &[(names::STREAM_NEXT_OFFSET, tail.as_str())],
            None,
        ),
    );
    // The model's own checks of a publish: the server makes none of them.
    let (mut tampered, _) = publish(token, 5, b"image");
    tampered.body = Arc::from(&b"tampered"[..]);
    check(
        PATH,
        &tampered,
        refusal(
            "a snapshot unlike its digest",
            400,
            ErrorCode::SnapshotDigestMismatch,
            &[],
            None,
        ),
    );
    let untyped = Request {
        content_type: None,
        ..publish(token, 5, b"image").0
    };
    check(
        PATH,
        &untyped,
        refusal(
            "a snapshot without a content type",
            400,
            ErrorCode::MissingContentType,
            &[],
            None,
        ),
    );
    let undigested = Request {
        snapshot_digest: None,
        ..publish(token, 5, b"image").0
    };
    check(
        PATH,
        &undigested,
        refusal(
            "a snapshot without a digest",
            400,
            ErrorCode::InvalidHeader,
            &[],
            None,
        ),
    );
    assert_eq!(world.send(&publish(token, 5, b"image").0).status, 204);
    assert_eq!(world.send(&retain(5)).status, 204);
    check(
        PATH,
        &publish(token, 4, b"image").0,
        refusal(
            "a publish below retention",
            410,
            ErrorCode::StreamGone,
            &[(names::STREAM_NEXT_OFFSET, tail.as_str())],
            Some("gone"),
        ),
    );
    check(
        PATH,
        &publish(token, 5, b"other").0,
        refusal(
            "another snapshot at the latest's offset",
            409,
            ErrorCode::SnapshotConflict,
            &[
                (names::STREAM_NEXT_OFFSET, tail.as_str()),
                (names::STREAM_SNAPSHOT_OFFSET, tail.as_str()),
            ],
            Some("snapshot-conflict"),
        ),
    );
    check(
        PATH,
        &snapshot_read(5, crate::wire::SnapshotPart::Chunk {
            first: 100,
            last: 200,
        }),
        refusal(
            "a range outside the snapshot",
            416,
            ErrorCode::RangeNotSatisfiable,
            &[
                (names::STREAM_INCARNATION, token_text.as_str()),
                (names::CONTENT_RANGE, "bytes */5"),
            ],
            None,
        ),
    );
    check(
        PATH,
        &read(tokened(token), Some(4)),
        refusal(
            "a read below retention",
            410,
            ErrorCode::StreamGone,
            incarnation,
            None,
        ),
    );

    // Parts, past the latest snapshot at 5.
    world.producer(&next(b"more"));
    let unknown = PartId::new("never-staged.part").unwrap();
    let probe = commit(token, 9, std::slice::from_ref(&unknown), digest_of(b"x"));
    for (what, request) in [
        ("a part without a cold store", stage_request(token, 9, b"x")),
        ("a commit without a cold store", probe.clone()),
        ("a discard without a cold store", discard(Some(token))),
    ] {
        let row = (request.method == Method::Post).then_some("no-cold-store");
        check(
            PATH,
            &request,
            refusal(what, 501, ErrorCode::NoColdStore, &[], row),
        );
    }
    with_cold_store(&world);
    check(
        PATH,
        &probe,
        refusal("an unknown part", 400, ErrorCode::UnknownPart, &[], None),
    );
    let miscounted = Request {
        body: Arc::from(&b""[..]),
        ..probe.clone()
    };
    check(
        PATH,
        &miscounted,
        refusal(
            "a malformed manifest",
            400,
            ErrorCode::InvalidPartsCommit,
            &[],
            None,
        ),
    );
    // The model's own check of a commit: the server makes none.
    let untyped = Request {
        content_type: None,
        ..probe.clone()
    };
    check(
        PATH,
        &untyped,
        refusal(
            "a commit without a content type",
            400,
            ErrorCode::MissingContentType,
            &[],
            None,
        ),
    );
    let part = stage(&world, token, 9, b"part").unwrap();
    check(
        PATH,
        &commit(token, 9, &[part], digest_of(b"other")),
        refusal(
            "parts unlike their digest",
            400,
            ErrorCode::SnapshotDigestMismatch,
            &[],
            None,
        ),
    );
    let part = stage(&world, token, 9, b"part").unwrap();
    let limits = world.server().limits();
    world.server().set_limits(Limits {
        max_snapshot_bytes: Some(1),
        max_staged_parts: 1,
        ..limits
    });
    check(
        PATH,
        &commit(token, 9, std::slice::from_ref(&part), digest_of(b"part")),
        refusal(
            "parts above the snapshot limit",
            413,
            ErrorCode::SnapshotTooLarge,
            &[(names::URSULA_MAX_SNAPSHOT_BYTES, "1")],
            None,
        ),
    );
    check(
        PATH,
        &stage_request(token, 9, b"x"),
        refusal(
            "a part past the staged bound",
            429,
            ErrorCode::StagedPartsLimit,
            &[
                (names::STREAM_INCARNATION, token_text.as_str()),
                (names::URSULA_MAX_STAGED_PARTS, "1"),
            ],
            Some("staged-parts-limit"),
        ),
    );
    let beyond = offset(9);
    check(
        PATH,
        &commit(token, 10, std::slice::from_ref(&unknown), digest_of(b"x")),
        refusal(
            "a commit beyond the tail",
            409,
            ErrorCode::SnapshotConflict,
            &[(names::STREAM_NEXT_OFFSET, beyond.as_str())],
            None,
        ),
    );

    // The request limit, then the closed stream.
    world.server().set_limits(Limits {
        max_request_bytes: 4,
        ..limits
    });
    check(
        PATH,
        &write(tokened(token), 1, 2, b"too large"),
        refusal(
            "a body above the request limit",
            413,
            ErrorCode::RequestTooLarge,
            &[(names::URSULA_MAX_REQUEST_BYTES, "4")],
            Some("too-large"),
        ),
    );
    world.server().set_limits(Limits::default());
    world.server().close(PATH).unwrap();
    check(
        PATH,
        &write(tokened(token), 1, 2, b"late"),
        refusal(
            "an append to a closed stream",
            409,
            ErrorCode::StreamClosed,
            &[(names::STREAM_CLOSED, "true")],
            Some("stream-closed"),
        ),
    );
}

/// A producer refusal: what it is, the request drawing it from a stream holding the claim
/// `(2, 0)`, its status, its code and its inventory row.
type ProducerCase = (
    &'static str,
    fn(&World, Incarnation) -> Request,
    u16,
    ErrorCode,
    &'static str,
);

/// The producer refusals with their codes (extensions: Producer Write Answers), each on a fresh
/// stream holding the claim `(2, 0)`.
#[test]
fn producer_refusals_carry_the_servers_error_body() {
    let cases: [ProducerCase; 4] = [
        (
            "a closed epoch",
            |world, token| {
                world.producer(&write(tokened(token), 3, 0, b"claim-3"));
                write(tokened(token), 2, 1, b"x")
            },
            403,
            ErrorCode::ProducerEpochClosed,
            "producer-epoch-closed",
        ),
        (
            "a stale epoch",
            |_, token| write(tokened(token), 1, 1, b"x"),
            403,
            ErrorCode::ProducerEpochStale,
            "producer-stale-epoch",
        ),
        (
            "an expired producer",
            |world, token| {
                world.server().advance(IDLE_RETIREMENT_MS);
                write(tokened(token), 2, 1, b"x")
            },
            403,
            ErrorCode::ProducerExpired,
            "producer-expired",
        ),
        (
            "an epoch never claimed",
            |_, token| write(tokened(token), 6, 1, b"x"),
            400,
            ErrorCode::InvalidProducer,
            "producer-epoch-not-claimed",
        ),
    ];
    for (what, refused, status, code, row) in cases {
        let world = World::new();
        let token = world.create();
        world.producer(&write(tokened(token), 2, 0, b"claim"));
        let request = refused(&world, token);
        assert_refused(
            &world,
            PATH,
            &request,
            &refusal(what, status, code, &[], Some(row)),
        );
    }
}

/// A part of the snapshot at `offset`, staged under `token`.
fn stage_request(token: Incarnation, offset: u64, body: &[u8]) -> Request {
    Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        body: Arc::from(body),
        ..Request::new(Method::Post, Target::SnapshotParts {
            offset: Offset::from_u64(offset),
        })
    }
}

/// Sends `raw`, a whole HTTP/1.1 request that closes its connection, to `address` and returns
/// the answer's status, headers (lowercase names) and body.
fn raw_exchange(
    address: std::net::SocketAddr,
    raw: &[u8],
) -> (u16, Vec<(String, String)>, Vec<u8>) {
    use std::io::Read;
    use std::io::Write;
    let mut connection = std::net::TcpStream::connect(address).unwrap();
    connection.write_all(raw).unwrap();
    let mut answer = Vec::new();
    connection.read_to_end(&mut answer).unwrap();
    let end = answer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    let (head, rest) = answer.split_at(end);
    let head = String::from_utf8(head.to_vec()).unwrap();
    let body = rest.strip_prefix(b"\r\n\r\n").unwrap().to_vec();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers = lines
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.trim().to_ascii_lowercase(), value.trim().to_owned())
        })
        .collect();
    (status, headers, body)
}

/// A refusal of the HTTP front: what it is, the raw request head, its status, its code and the
/// headers it names.
type FrontCase<'a> = (&'a str, String, u16, ErrorCode, &'a [(&'a str, &'a str)]);

/// The refusals the HTTP front decides before the server sees a request, through
/// `http::serve`: each with the server's error body, its code and `Cache-Control: no-store`.
#[test]
fn the_http_front_refuses_with_the_servers_error_body() {
    let world = World::new();
    let token = world.create();
    world.server().set_limits(Limits {
        max_request_bytes: 4,
        ..Limits::default()
    });
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = Arc::clone(&world.server);
    // The serving thread lives as long as the test process.
    drop(std::thread::spawn(move || {
        super::http::serve(listener, server)
    }));
    let zero = Offset::ZERO.to_string();
    let cases: [FrontCase<'_>; 10] = [
        (
            "an unknown method",
            format!("BREW {PATH} HTTP/1.1\r\n"),
            405,
            ErrorCode::MethodNotAllowed,
            &[],
        ),
        // The model's own check: the server reads no length as an empty body.
        (
            "a POST without a length",
            format!("POST {PATH} HTTP/1.1\r\n"),
            411,
            ErrorCode::InvalidRequest,
            &[],
        ),
        // Not modeled: the server reads a chunked body.
        (
            "a chunked body",
            format!("POST {PATH} HTTP/1.1\r\ntransfer-encoding: chunked\r\n"),
            501,
            ErrorCode::Internal,
            &[],
        ),
        (
            "a malformed offset",
            format!("GET {PATH}/snapshot/12 HTTP/1.1\r\n"),
            400,
            ErrorCode::InvalidOffset,
            &[],
        ),
        // The model's own check: the server ignores an unknown parameter.
        (
            "an unknown query parameter",
            format!("GET {PATH}?cursor=1 HTTP/1.1\r\n"),
            400,
            ErrorCode::InvalidQuery,
            &[],
        ),
        (
            "a malformed producer key",
            format!(
                "POST {PATH} HTTP/1.1\r\ncontent-length: 0\r\nproducer-id: {PRODUCER_ID}\r\n\
                 producer-epoch: one\r\nproducer-seq: 0\r\n"
            ),
            400,
            ErrorCode::InvalidProducer,
            &[],
        ),
        (
            "a malformed precondition on an append",
            format!("POST {PATH} HTTP/1.1\r\ncontent-length: 0\r\nstream-incarnation: soon\r\n"),
            400,
            ErrorCode::InvalidHeader,
            &[],
        ),
        (
            "a malformed precondition",
            format!("GET {PATH}/snapshot/{zero} HTTP/1.1\r\nstream-incarnation: soon\r\n"),
            400,
            ErrorCode::InvalidHeader,
            &[],
        ),
        (
            "a malformed parts commit",
            format!(
                "PUT {PATH}/snapshot/{zero} HTTP/1.1\r\ncontent-length: 0\r\n\
                 stream-incarnation: {token}\r\nstream-snapshot-parts: many\r\n"
            ),
            400,
            ErrorCode::InvalidPartsCommit,
            &[],
        ),
        (
            "a body above the request limit",
            format!("POST {PATH} HTTP/1.1\r\ncontent-length: 10\r\n\r\n0123456789"),
            413,
            ErrorCode::RequestTooLarge,
            &[(names::URSULA_MAX_REQUEST_BYTES, "4")],
        ),
    ];
    for (what, head, status, code, headers) in cases {
        let mut raw = head.into_bytes();
        // Every request but the one with a body ends its head here.
        if !raw.ends_with(b"0123456789") {
            raw.extend_from_slice(b"connection: close\r\n\r\n");
        }
        let (got, answer_headers, body) = raw_exchange(address, &raw);
        check_refusal(
            &refusal(what, status, code, headers, None),
            got,
            &answer_headers,
            &body,
            None,
        );
    }
}
