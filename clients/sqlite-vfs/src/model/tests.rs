//! The model through its production entry point, the in-process transport: every answer is
//! rendered with `ursula_headers` and parsed back by `wire`, as a client reads it. Rows cite
//! #581 §2.3.1.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use ursula_headers::ClosedBy;
use ursula_headers::ClosedEpoch;
use ursula_headers::ClusterId;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::HeaderError;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationMismatch;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Inherited;
use ursula_headers::MAX_INCARNATION;
use ursula_headers::Offset;
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
    // Addendum 3 item 3: the floor binds untokened claims too, and claims with no record.
    world.server().advance(IDLE_RETIREMENT_MS);
    assert_eq!(
        world.producer(&write(None, 2, 0, b"base client")),
        ProducerAnswer::ProducerExpired { epoch: epoch(5) }
    );
    assert_eq!(
        world.producer(&write(tokened(token), 3, 0, b"zombie")),
        ProducerAnswer::ProducerExpired { epoch: epoch(5) }
    );
    assert_eq!(
        world.producer(&write(None, 2, 3, b"no record")),
        ProducerAnswer::ProducerExpired { epoch: epoch(5) }
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
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim"));
    let commit = write(tokened(token), 1, 1, b"commit");
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

#[test]
fn addendum_4_an_empty_write_on_an_applied_key_gets_its_row() {
    let world = World::new();
    let token = world.create();
    world.producer(&write(tokened(token), 1, 0, b"claim"));
    let commit = write(tokened(token), 1, 1, b"commit");
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
        world.producer(&with_body(&write(tokened(token), 1, 0, b"claim"), b"")),
        ProducerAnswer::ReceiptEvicted { .. }
    ));
    // Row 8: the empty body's digest under the newest write's key.
    assert_eq!(
        world.producer(&write(tokened(token), 1, 1, b"")),
        ProducerAnswer::KeyConflict { epoch: epoch(1) }
    );
    // Row 13 refuses before the content checks.
    assert_eq!(
        world.producer(&write(tokened(token), 1, 5, b"")),
        ProducerAnswer::SeqGap {
            expected: seq(2),
            received: seq(5),
        }
    );
    // Row 12 would apply: only then is the empty body refused.
    let answer = world.send(&write(tokened(token), 1, 2, b""));
    assert_eq!((answer.status, answer.typed), (400, Ok(Typed::Untyped)));
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
    // The snapshot reads back with its headers.
    let read = world.send(&Request::snapshot_read(Offset::from_u64(64), 1 << 20));
    let Ok(Typed::SnapshotRead(snapshot)) = read.typed else {
        panic!("{read:?}");
    };
    assert!(snapshot.up_to_date);
    assert_eq!(read.body, b"image at 64");
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
    // §4.8: no write since the event: the previous token and the current tail.
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
fn w5_an_accepted_loss_inherits_the_surviving_prefix() {
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
fn w5_two_events_name_one_predecessor() {
    // Without a write between them: the token before both.
    let world = World::new();
    let old = world.create();
    world.server().foreign_append(PATH, &[1; 8]).unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    assert_eq!(
        world
            .stream_head()
            .inherited()
            .map(|i| i.previous_incarnation),
        Some(old)
    );
    // With a write between them: the token of the history between.
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
fn w5_a_history_event_fences_the_producer_by_history() {
    let world = World::new();
    let old = world.create();
    world.producer(&write(tokened(old), 1, 0, b"claim"));
    world.producer(&write(tokened(old), 1, 1, b"one"));
    world
        .server()
        .history_event(HistoryEvent::NewHistory)
        .unwrap();
    let new = world.server().token(PATH).unwrap();
    // The old token gets 412 first (G2).
    assert_eq!(world.send(&write(tokened(old), 1, 2, b"two")).status, 412);
    // Row 11 under the new token: the epoch is closed at its surviving last seq.
    let ProducerAnswer::EpochClosed(closure) = world.producer(&write(tokened(new), 1, 2, b"two"))
    else {
        panic!();
    };
    assert_eq!(closure.by(), ClosedBy::History);
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
