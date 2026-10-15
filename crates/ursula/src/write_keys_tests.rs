//! Write keys over HTTP (#581 section 2): every typed producer answer as the
//! shared header crate parses it, `Producer-Digest`, create without producer
//! headers, the epoch floor, and answers decided before the body (W7).
//!
//! Every producer answer is parsed with [`ProducerAnswer::parse`], which
//! refuses a producer `403` without `Producer-Outcome` and any producer
//! header without `Producer-Epoch`, so each test also checks that the server
//! renders what its clients parse.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::Version;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use tower::ServiceExt;
use ursula_headers::ClosedEpoch;
use ursula_headers::CurrentEpoch;
use ursula_headers::Delivery;
use ursula_headers::EpochClosure;
use ursula_headers::ProducerAnswer;
use ursula_headers::Receipt;
use ursula_headers::value::Epoch;
use ursula_headers::value::Incarnation;
use ursula_headers::value::Offset;
use ursula_headers::value::Seq;

use crate::HttpState;
use crate::IngressAdmission;
use crate::Persistence;
use crate::Topology;
use crate::WallClock;
use crate::client_router_with_admission;
use crate::spawn_runtime;

const URI: &str = "/benchcmp/keys";
/// A producer record idle this long is retired at its next write
/// (bounded-stream-state F3).
const PRODUCER_IDLE_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

#[derive(Clone)]
struct Clock(Arc<AtomicU64>);

impl WallClock for Clock {
    fn unix_time_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn app(now_ms: &Arc<AtomicU64>) -> Router {
    let mut config = ursula_config::UrsulaConfig::default();
    config.runtime.core_count = 1;
    config.raft.group_count = 1;
    let runtime = spawn_runtime(&config, Persistence::InMemory, Topology::SingleNode {
        raft_group_count: 1,
    })
    .expect("runtime")
    .runtime;
    let state = HttpState::new(runtime).with_wall_clock(Clock(Arc::clone(now_ms)));
    client_router_with_admission(state, IngressAdmission::default())
}

/// A request body of `body` that records in `polled` whether the server
/// polled it.
fn watched(body: &str, polled: &Arc<AtomicBool>) -> Body {
    let polled = Arc::clone(polled);
    let mut body = Some(Bytes::copy_from_slice(body.as_bytes()));
    Body::from_stream(futures_util::stream::poll_fn(move |_cx| {
        polled.store(true, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Ready(body.take().map(Ok::<_, std::convert::Infallible>))
    }))
}

struct Write<'a> {
    epoch: u64,
    seq: u64,
    body: &'a str,
    digest: bool,
    incarnation: Option<&'a str>,
    version: Version,
    /// Sends `Stream-Closed: true`.
    closes: bool,
    /// Declares the body's length in `Content-Length`.
    declared: bool,
    /// The text whose digest `Producer-Digest` names, when not the body's.
    digest_of: Option<&'a str>,
    /// Sends no `Content-Type`.
    untyped: bool,
}

impl<'a> Write<'a> {
    fn new(epoch: u64, seq: u64, body: &'a str) -> Self {
        Self {
            epoch,
            seq,
            body,
            digest: false,
            incarnation: None,
            version: Version::HTTP_11,
            closes: false,
            declared: false,
            digest_of: None,
            untyped: false,
        }
    }

    fn digest(self) -> Self {
        Self {
            digest: true,
            ..self
        }
    }

    fn tokened(self, incarnation: &'a str) -> Self {
        Self {
            incarnation: Some(incarnation),
            ..self
        }
    }

    fn http2(self) -> Self {
        Self {
            version: Version::HTTP_2,
            ..self
        }
    }

    fn closing(self) -> Self {
        Self {
            closes: true,
            ..self
        }
    }

    fn declared(self) -> Self {
        Self {
            declared: true,
            ..self
        }
    }

    /// `Producer-Digest` of `text` instead of the body.
    fn digest_of(self, text: &'a str) -> Self {
        Self {
            digest: true,
            digest_of: Some(text),
            ..self
        }
    }

    fn untyped(self) -> Self {
        Self {
            untyped: true,
            ..self
        }
    }
}

/// Sends `write`, and returns its response and whether its body was polled.
async fn send(app: &Router, write: &Write<'_>) -> (Response, bool) {
    let polled = Arc::new(AtomicBool::new(false));
    let mut request = Request::builder()
        .method("POST")
        .uri(URI)
        .version(write.version)
        .header("producer-id", "writer")
        .header("producer-epoch", write.epoch.to_string())
        .header("producer-seq", write.seq.to_string());
    if !write.untyped {
        request = request.header(CONTENT_TYPE, "text/plain");
    }
    if write.digest {
        let named = write.digest_of.unwrap_or(write.body);
        request = request.header(
            "producer-digest",
            blake3::hash(named.as_bytes()).to_hex().to_string(),
        );
    }
    if let Some(incarnation) = write.incarnation {
        request = request.header("stream-incarnation", incarnation);
    }
    if write.closes {
        request = request.header("stream-closed", "true");
    }
    if write.declared {
        request = request.header("content-length", write.body.len().to_string());
    }
    let body = if write.body.is_empty() {
        polled.store(true, Ordering::SeqCst);
        Body::empty()
    } else {
        watched(write.body, &polled)
    };
    let response = app
        .clone()
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("response");
    (response, polled.load(Ordering::SeqCst))
}

/// The typed producer answer of `response`, checked against its status.
#[track_caller]
fn answer(response: &Response) -> ProducerAnswer {
    for name in ["x-ursula-not-applied", "x-ursula-not-applied-checked"] {
        assert!(!response.headers().contains_key(name), "{name}");
    }
    let parsed = ProducerAnswer::parse(
        response.status().as_u16(),
        response
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .expect("a well-formed producer answer")
    .expect("a producer answer");
    assert_eq!(parsed.status(), response.status().as_u16());
    parsed
}

async fn create(app: &Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(URI)
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    response
        .headers()
        .get("stream-incarnation")
        .expect("incarnation")
        .to_str()
        .expect("ascii")
        .to_owned()
}

async fn tail(app: &Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(URI)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    response
        .headers()
        .get("stream-next-offset")
        .expect("tail")
        .to_str()
        .expect("ascii")
        .to_owned()
}

fn epoch(value: u64) -> Epoch {
    Epoch::try_from(value).expect("epoch")
}

fn seq(value: u64) -> Seq {
    Seq::try_from(value).expect("seq")
}

/// A receipt answer for `epoch`'s write at `start..next`, `seq` the
/// epoch's highest applied seq, and the epoch the claim closed, if any.
fn receipt(
    delivery: Delivery,
    (current, highest): (u64, u64),
    (start, next): (u64, u64),
    closed: Option<(u64, u64)>,
    incarnation: Incarnation,
) -> ProducerAnswer {
    let current = match closed {
        None => CurrentEpoch::new(epoch(current)),
        Some((closed_epoch, closed_seq)) => CurrentEpoch::closing(epoch(current), ClosedEpoch {
            epoch: epoch(closed_epoch),
            seq: seq(closed_seq),
        })
        .expect("an earlier epoch"),
    };
    ProducerAnswer::Receipt {
        delivery,
        epoch: current,
        seq: seq(highest),
        receipt: Receipt::new(Offset::from_u64(start), Offset::from_u64(next), false)
            .expect("receipt"),
        incarnation,
    }
}

#[tokio::test]
async fn every_answer_of_the_evaluation_table_is_typed_on_the_wire() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let app = app(&now_ms);
    let token = create(&app).await;
    let incarnation = Incarnation::parse("stream-incarnation", &token).expect("token");
    let send = |write: Write<'static>| {
        let app = app.clone();
        async move { answer(&send(&app, &write).await.0) }
    };
    use Delivery::Applied;
    use Delivery::Repeat;

    // Rows 2 and 12: applied now, `200`, `Producer-Seq` the highest applied.
    assert_eq!(
        send(Write::new(1, 0, "a")).await,
        receipt(Applied, (1, 0), (0, 1), None, incarnation)
    );
    assert_eq!(
        send(Write::new(1, 1, "bb")).await,
        receipt(Applied, (1, 1), (1, 3), None, incarnation)
    );
    // Row 9: the newest write's receipt. Row 10: an older write is
    // `receipt-evicted`, with the highest seq (the conformance case).
    assert_eq!(
        send(Write::new(1, 1, "bb")).await,
        receipt(Repeat, (1, 1), (1, 3), None, incarnation)
    );
    assert_eq!(
        send(Write::new(1, 0, "a")).await,
        ProducerAnswer::ReceiptEvicted {
            epoch: CurrentEpoch::new(epoch(1)),
            seq: seq(1),
            incarnation,
        }
    );
    // Row 13: a gap. Row 7: a later epoch above seq 0.
    assert_eq!(send(Write::new(1, 5, "x")).await, ProducerAnswer::SeqGap {
        expected: seq(2),
        received: seq(5),
    });
    assert_eq!(
        send(Write::new(2, 1, "x")).await,
        ProducerAnswer::EpochNotClaimed
    );
    // Row 6: a claim reports the epoch it closed, on every repeat.
    assert_eq!(
        send(Write::new(2, 0, "c")).await,
        receipt(Applied, (2, 0), (3, 4), Some((1, 1)), incarnation)
    );
    assert_eq!(
        send(Write::new(2, 0, "c")).await,
        receipt(Repeat, (2, 0), (3, 4), Some((1, 1)), incarnation)
    );
    // Row 4: an applied write of the closed epoch is a `403`, typed with
    // the closure. Row 5: an older epoch is the base protocol's stale epoch.
    assert_eq!(
        send(Write::new(1, 1, "bb")).await,
        ProducerAnswer::EpochClosed(
            EpochClosure::by_claim(epoch(2), ClosedEpoch {
                epoch: epoch(1),
                seq: seq(1),
            })
            .expect("closure")
        )
    );
    assert_eq!(send(Write::new(3, 0, "d")).await.status(), 200);
    assert_eq!(
        send(Write::new(1, 1, "bb")).await,
        ProducerAnswer::StaleEpoch { epoch: epoch(3) }
    );
    // Row 8: another identity under a held key.
    assert_eq!(
        send(Write::new(3, 0, "d").digest()).await,
        ProducerAnswer::KeyConflict { epoch: epoch(3) }
    );
    // Row 3: a producer without a record.
    let response = send_as(&app, "fresh", 0, 4).await;
    assert_eq!(answer(&response), ProducerAnswer::SeqGap {
        expected: seq(0),
        received: seq(4),
    });
    // Row 1: past idle expiry, a tokened write at or below the forgotten
    // epoch gets the floor.
    now_ms.fetch_add(PRODUCER_IDLE_EXPIRY_MS, Ordering::Relaxed);
    let (response, _) = self::send(&app, &Write::new(3, 1, "e").tokened(&token)).await;
    assert_eq!(answer(&response), ProducerAnswer::ProducerExpired {
        epoch: epoch(3)
    });
    let (response, _) = self::send(&app, &Write::new(4, 0, "e").tokened(&token)).await;
    assert_eq!(answer(&response).status(), 200);
}

/// A write by another producer id, untokened.
async fn send_as(app: &Router, id: &str, epoch: u64, seq: u64) -> Response {
    let request = Request::builder()
        .method("POST")
        .uri(URI)
        .header(CONTENT_TYPE, "text/plain")
        .header("producer-id", id)
        .header("producer-epoch", epoch.to_string())
        .header("producer-seq", seq.to_string())
        .body(Body::from("z"))
        .expect("request");
    app.clone().oneshot(request).await.expect("response")
}

#[tokio::test]
async fn a_digest_that_does_not_match_the_body_applies_nothing() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let app = app(&now_ms);
    create(&app).await;
    let before = tail(&app).await;
    let request = Request::builder()
        .method("POST")
        .uri(URI)
        .header(CONTENT_TYPE, "text/plain")
        .header("producer-id", "writer")
        .header("producer-epoch", "0")
        .header("producer-seq", "0")
        .header(
            "producer-digest",
            blake3::hash(b"another body").to_hex().to_string(),
        )
        .body(Body::from("abc"))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(tail(&app).await, before);
    // The matching digest applies.
    let (response, _) = send(&app, &Write::new(0, 0, "abc").digest()).await;
    assert_eq!(answer(&response).status(), 200);
}

#[tokio::test]
async fn a_create_with_producer_headers_is_refused() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let app = app(&now_ms);
    let request = Request::builder()
        .method("PUT")
        .uri(URI)
        .header(CONTENT_TYPE, "text/plain")
        .header("producer-id", "writer")
        .header("producer-epoch", "0")
        .header("producer-seq", "0")
        .body(Body::from("abc"))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(URI)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(head.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_tokened_repeat_or_key_conflict_is_answered_before_the_body() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let app = app(&now_ms);
    let token = create(&app).await;
    let (first, _) = send(&app, &Write::new(0, 0, "abc").digest().tokened(&token)).await;
    let first = answer(&first);
    let after = tail(&app).await;

    // On HTTP/2 the answer goes first: the body is never polled.
    let (repeat, polled) = send(
        &app,
        &Write::new(0, 0, "abc").digest().tokened(&token).http2(),
    )
    .await;
    assert!(!polled, "an early answer polls no body");
    let ProducerAnswer::Receipt {
        delivery: Delivery::Applied,
        epoch,
        seq,
        receipt,
        incarnation,
    } = first
    else {
        panic!("expected a receipt, got {first:?}");
    };
    assert_eq!(answer(&repeat), ProducerAnswer::Receipt {
        delivery: Delivery::Repeat,
        epoch,
        seq,
        receipt,
        incarnation,
    });
    let (conflict, polled) = send(
        &app,
        &Write::new(0, 0, "xyz").digest().tokened(&token).http2(),
    )
    .await;
    assert!(!polled, "a key conflict polls no body");
    assert_eq!(answer(&conflict), ProducerAnswer::KeyConflict {
        epoch: Epoch::ZERO
    });

    // On HTTP/1.1 without `Expect`, a small body is read and decided at
    // apply: the lookup would save nothing.
    let (repeat, polled) = send(&app, &Write::new(0, 0, "abc").digest().tokened(&token)).await;
    assert!(polled, "a small HTTP/1.1 body is read");
    assert_eq!(repeat.status(), StatusCode::NO_CONTENT);
    assert!(!repeat.headers().contains_key("connection"));

    // Untokened, or under another token, the write is read and decided at
    // apply.
    let (repeat, polled) = send(&app, &Write::new(0, 0, "abc").digest().http2()).await;
    assert!(polled, "an untokened write is read");
    assert_eq!(repeat.status(), StatusCode::NO_CONTENT);
    let (stale, polled) = send(
        &app,
        &Write::new(0, 0, "abc").tokened("4503599627370497").http2(),
    )
    .await;
    assert!(polled, "a write under another token is read");
    assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(tail(&app).await, after);
}

/// The early and the late path render one answer for the same request, and
/// the early path answers nothing that the late path refuses with `400`
/// (#581 section 2.5).
#[tokio::test]
async fn early_and_late_answers_agree() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let app = app(&now_ms);
    let token = create(&app).await;
    let big = "x".repeat(70_000);
    let (first, _) = send(&app, &Write::new(0, 0, &big).tokened(&token)).await;
    assert_eq!(first.status(), StatusCode::OK);
    let (first, _) = send(&app, &Write::new(0, 1, "y").tokened(&token)).await;
    assert_eq!(first.status(), StatusCode::OK);

    // A repeat of the newest write, and of an older one, early (HTTP/2) and
    // late (a small HTTP/1.1 body): the same headers.
    for (seq, body) in [(1, "y"), (0, big.as_str())] {
        let (early, early_polled) =
            send(&app, &Write::new(0, seq, body).tokened(&token).http2()).await;
        let (late, late_polled) = send(&app, &Write::new(0, seq, "z").tokened(&token)).await;
        assert!(!early_polled && late_polled, "{seq}");
        assert_eq!(early.status(), late.status(), "{seq}");
        assert_eq!(early.headers(), late.headers(), "{seq}");
        answer(&early);
    }
    // A declared body of at least 64 KiB over HTTP/1.1 takes the early path
    // and is drained, keeping the connection.
    let (repeat, _) = send(&app, &Write::new(0, 0, &big).tokened(&token).declared()).await;
    assert_eq!(repeat.status(), StatusCode::NO_CONTENT);
    assert!(!repeat.headers().contains_key("connection"));

    // A close-only repeat of a key whose write was an append: the receipt,
    // and no `Stream-Closed` on the open stream, on both paths.
    for write in [
        Write::new(0, 1, "").tokened(&token).closing().http2(),
        Write::new(0, 1, "").tokened(&token).closing(),
    ] {
        let (repeat, _) = send(&app, &write).await;
        assert_eq!(repeat.status(), StatusCode::NO_CONTENT);
        assert!(!repeat.headers().contains_key("stream-closed"));
        assert!(matches!(answer(&repeat), ProducerAnswer::Receipt { .. }));
    }

    // An empty write that does not close gets its key's row on both paths:
    // a repeat of an applied key is its receipt, and only a write that would
    // apply is `400 EmptyAppend` (#581 section 5.3, step 5).
    assert_same_answer(
        &app,
        Write::new(0, 1, "").tokened(&token).http2(),
        Write::new(0, 1, "").tokened(&token),
        StatusCode::NO_CONTENT,
    )
    .await;
    assert_same_answer(
        &app,
        Write::new(0, 2, "").tokened(&token).http2(),
        Write::new(0, 2, "").tokened(&token),
        StatusCode::BAD_REQUEST,
    )
    .await;
}

/// Sends `early` (a request the early path takes) and `late` (a small
/// HTTP/1.1 one), and checks that both get `status` with the same headers.
async fn assert_same_answer(app: &Router, early: Write<'_>, late: Write<'_>, status: StatusCode) {
    let (early_answer, _) = send(app, &early).await;
    let (late_answer, late_polled) = send(app, &late).await;
    assert!(late_polled, "the late path reads the body");
    assert_eq!(
        early_answer.status(),
        status,
        "early: {:?}",
        early_answer.headers()
    );
    assert_eq!(
        late_answer.status(),
        status,
        "late: {:?}",
        late_answer.headers()
    );
    // A late refusal carries the state machine's message as its body.
    let headers = |response: &Response| {
        let mut headers = response.headers().clone();
        headers.remove(CONTENT_TYPE);
        headers.remove(axum::http::header::CONTENT_LENGTH);
        headers
    };
    assert_eq!(headers(&early_answer), headers(&late_answer));
}

/// D1 and D2 of the evaluation order (#581 section 5.3): the producer rows
/// use the `Producer-Digest` header and run before every content check, on
/// every transport.
#[tokio::test]
async fn producer_rows_come_before_content_checks_on_both_paths() {
    let now_ms = Arc::new(AtomicU64::new(1_000));
    let app = app(&now_ms);
    let token = create(&app).await;
    let (first, _) = send(&app, &Write::new(0, 0, "held").digest().tokened(&token)).await;
    assert_eq!(first.status(), StatusCode::OK);
    let before = tail(&app).await;

    // D1: another digest on the held key is a key conflict, whether the body
    // matches its digest or not, never a `400`.
    assert_same_answer(
        &app,
        Write::new(0, 0, "other").digest().tokened(&token).http2(),
        Write::new(0, 0, "other").digest().tokened(&token),
        StatusCode::FORBIDDEN,
    )
    .await;
    assert_same_answer(
        &app,
        Write::new(0, 0, "held")
            .digest_of("other")
            .tokened(&token)
            .http2(),
        Write::new(0, 0, "held").digest_of("other").tokened(&token),
        StatusCode::FORBIDDEN,
    )
    .await;
    let (conflict, _) = send(
        &app,
        &Write::new(0, 0, "x").digest_of("other").tokened(&token),
    )
    .await;
    assert_eq!(answer(&conflict), ProducerAnswer::KeyConflict {
        epoch: Epoch::ZERO
    });

    // D2: a repeat with no `Content-Type`, or with a body that does not match
    // its own digest, still gets its receipt.
    assert_same_answer(
        &app,
        Write::new(0, 0, "held")
            .digest()
            .untyped()
            .tokened(&token)
            .http2(),
        Write::new(0, 0, "held").digest().untyped().tokened(&token),
        StatusCode::NO_CONTENT,
    )
    .await;
    let (repeat, _) = send(
        &app,
        &Write::new(0, 0, "held").digest().untyped().tokened(&token),
    )
    .await;
    assert!(matches!(answer(&repeat), ProducerAnswer::Receipt { .. }));

    // A write that would apply gets its content checks: no `Content-Type`,
    // or a body that does not match its digest, is `400`.
    for write in [
        Write::new(0, 1, "next").untyped().tokened(&token),
        Write::new(0, 1, "next").digest_of("other").tokened(&token),
    ] {
        let (refused, _) = send(&app, &write).await;
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(tail(&app).await, before);
}
