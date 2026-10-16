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
use ursula_headers::names;

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
    // A range that starts past the end: `416`, naming the length.
    let past = chunk(11, 20);
    assert_eq!(past.status, 416);
    assert_eq!(past.typed, Ok(Typed::Untyped));
    assert!(past.body.is_empty());
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
    let missing = read(None, Some(0));
    assert_eq!(world.send(&missing).status, 404);
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
    // A wrong token below retention, past the tail, then within range.
    assert_eq!(world.send(&read(other(token), Some(10))).status, 410);
    assert_eq!(world.send(&read(other(token), Some(101))).status, 416);
    assert_eq!(world.send(&read(other(token), Some(60))).status, 412);
    assert_eq!(world.send(&read(tokened(token), Some(101))).status, 416);
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
    let answer = world.send(&Request {
        keys: ProducerRequestHeaders {
            producer: None,
            precondition: tokened(token),
        },
        body: Arc::from(body),
        ..Request::new(Method::Post, Target::SnapshotParts {
            offset: Offset::from_u64(offset),
        })
    });
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
// on the model's clock; Busy and InProgress are scripted 503s with `Retry-After`.
#[test]
fn parts_need_a_cold_store_and_are_swept_after_a_day() {
    let world = World::new();
    let token = world.create();
    world.server().foreign_append(PATH, &[0; 64]).unwrap();
    assert_eq!(stage(&world, token, 64, b"x"), Err(501));
    let unknown = PartId::new("never-staged.part").unwrap();
    let probe = commit(token, 64, std::slice::from_ref(&unknown), digest_of(b"x"));
    assert_eq!(world.send(&probe).status, 501);
    with_cold_store(&world);
    let part = stage(&world, token, 64, b"x").unwrap();
    world.server().advance(super::PART_GRACE_MS - 1);
    assert_eq!(world.server().staged_parts(), 1);
    world.server().advance(1);
    let swept = world.send(&commit(token, 64, &[part], digest_of(b"x")));
    assert_eq!(swept.status, 400, "swept");
    assert_eq!(world.server().staged_parts(), 0);
    for (refusal, code) in [
        (super::faults::CommitRefusal::Busy, "SnapshotCommitsBusy"),
        (
            super::faults::CommitRefusal::InProgress,
            "SnapshotCommitInProgress",
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
        assert!(String::from_utf8_lossy(&refused.body).contains(code));
    }
}
