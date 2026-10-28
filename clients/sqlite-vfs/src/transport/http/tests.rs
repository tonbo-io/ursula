//! [`HttpTransport`] over loopback: against the model server, and against scripted sockets.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use ursula_headers::ClusterId;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Offset;
use ursula_headers::PartCount;
use ursula_headers::PartsCommit;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::SnapshotDigest;

use super::HttpTransport;
use super::Timeouts;
use super::no_answer_from;
use crate::codec::digest::SnapshotDigester;
use crate::codec::frame::CONTENT_TYPE;
use crate::model::Limits;
use crate::model::ModelServer;
use crate::model::faults::Fault;
use crate::model::faults::Scripted;
use crate::model::http;
use crate::model::tests::close_only;
use crate::model::tests::create;
use crate::model::tests::with_body;
use crate::model::tests::write;
use crate::transport::Attempt;
use crate::transport::Transport;
use crate::transport::http::mock::answer;
use crate::transport::http::mock::refusing;
use crate::transport::http::mock::request_header;
use crate::transport::http::mock::serve;
use crate::transport::model::ModelTransport;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::Typed;

const PATH: &str = "/v1/stream/b/db";

fn model() -> Arc<Mutex<ModelServer>> {
    Arc::new(Mutex::new(ModelServer::new(
        ClusterId::new("model-a").unwrap(),
        7,
    )))
}

/// A model served over loopback, and its stream's URL.
fn served() -> (Arc<Mutex<ModelServer>>, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}{PATH}", listener.local_addr().unwrap());
    let server = model();
    let shared = Arc::clone(&server);
    // The serving thread lives as long as the test process.
    drop(std::thread::spawn(move || http::serve(listener, shared)));
    (server, url)
}

fn tokened(server: &Mutex<ModelServer>) -> Option<IncarnationPrecondition> {
    let token = server.lock().unwrap().token(PATH)?;
    Some(IncarnationPrecondition { incarnation: token })
}

fn keys(precondition: Option<IncarnationPrecondition>) -> ProducerRequestHeaders {
    ProducerRequestHeaders {
        producer: None,
        precondition,
    }
}

/// Step `step` of requests that draw every kind of answer the model gives, sent in order to a
/// fresh model. Some steps first change the model's limits or state.
fn script(server: &Mutex<ModelServer>, step: usize) -> Option<Request> {
    let token = || tokened(server);
    let snapshot = |offset: u64, body: &[u8]| {
        let mut digester = SnapshotDigester::new(CONTENT_TYPE);
        digester.update(body);
        Request {
            keys: keys(token()),
            content_type: Some(CONTENT_TYPE.to_owned()),
            snapshot_digest: Some(digester.finalize()),
            body: Arc::from(body),
            ..Request::new(Method::Put, Target::Snapshot {
                offset: Offset::from_u64(offset),
            })
        }
    };
    let request = match step {
        0 | 14 | 22 => Request::new(Method::Head, Target::Stream),
        1 | 2 => create(),
        3 | 7 => write(token(), 1, 0, b"claim"),
        4 | 5 => write(token(), 1, 1, b"commit one"),
        6 => write(token(), 1, 1, b"another body"),
        8 => write(token(), 1, 5, b"gap"),
        9 => write(token(), 2, 3, b"unclaimed"),
        10 => write(token(), 2, 0, b"claim two"),
        11 => write(token(), 1, 2, b"closed"),
        12 => write(token(), 0, 0, b"expired"),
        13 => write(None, 9, 4, b"unclaimed, untokened"),
        15 => Request {
            keys: keys(token()),
            ..Request::read(None, true, 1 << 20)
        },
        16 => snapshot(5, b"image"),
        17 => snapshot(5, b"other image"),
        18 => Request::snapshot_read(
            Offset::from_u64(5),
            token()?.incarnation,
            crate::wire::SnapshotPart::Header,
        ),
        19 => Request {
            keys: keys(token()),
            ..Request::new(Method::Put, Target::Retention {
                offset: Offset::from_u64(5),
            })
        },
        24 => Request {
            keys: keys(token()),
            ..Request::new(Method::Put, Target::Retention {
                offset: Offset::from_u64(6),
            })
        },
        20 => {
            let mut stale = write(token(), 2, 1, b"x");
            stale.keys.precondition = Some(IncarnationPrecondition {
                incarnation: Incarnation::try_from(1 << 52).unwrap(),
            });
            stale
        }
        21 => {
            server.lock().unwrap().set_limits(Limits {
                max_request_bytes: 8,
                max_snapshot_bytes: Some(1 << 30),
                ..Limits::default()
            });
            write(token(), 2, 1, b"over the limit")
        }
        23 => {
            server.lock().unwrap().close(PATH).unwrap();
            write(token(), 2, 1, b"closed")
        }
        _ => return None,
    };
    Some(request)
}

/// A control request to the model behind `url`, over a connection of its own: the answer's body.
fn control(url: &str, method: &str, route: &str) -> String {
    use std::io::Read;
    use std::io::Write;
    let addr = url.trim_start_matches("http://").split('/').next().unwrap();
    let mut connection = std::net::TcpStream::connect(addr).unwrap();
    write!(
        connection,
        "{method} /__model/{route} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut answer = String::new();
    connection.read_to_string(&mut answer).unwrap();
    assert!(answer.starts_with("HTTP/1.1 2"), "{answer}");
    answer
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_owned()
}

// `/__model/hold` holds a snapshot PUT before the model sees it, for as long as the test needs,
// other requests going on; `/__model/release` lets it through.
#[test]
fn a_held_snapshot_put_waits_for_the_release() {
    let (server, url) = served();
    let http = HttpTransport::new(url.clone());
    let send = |request: &Request| match http.send(request, Attempt::default()) {
        Exchange::Answered(answer) => *answer,
        Exchange::NoAnswer(no) => panic!("{no:?}"),
    };
    assert_eq!(send(&create()).status, 201);
    control(&url, "POST", "hold");
    let token = server.lock().unwrap().token(PATH).unwrap();
    let body = b"image".to_vec();
    let mut digester = SnapshotDigester::new(CONTENT_TYPE);
    digester.update(&body);
    let publish = Request {
        keys: keys(Some(IncarnationPrecondition { incarnation: token })),
        content_type: Some(CONTENT_TYPE.to_owned()),
        snapshot_digest: Some(digester.finalize()),
        body: Arc::from(body),
        ..Request::new(Method::Put, Target::Snapshot {
            offset: Offset::ZERO,
        })
    };
    let held = {
        let url = url.clone();
        std::thread::spawn(move || HttpTransport::new(url).send(&publish, Attempt::default()))
    };
    let mut waited = 0;
    while control(&url, "GET", "held") != "1" {
        assert!(waited < 2_000, "the PUT was never held");
        std::thread::sleep(Duration::from_millis(5));
        waited += 5;
    }
    let snapshot = || match send(&Request::new(Method::Head, Target::Stream)).typed {
        Ok(Typed::Head {
            stream: Some(stream),
            ..
        }) => stream.snapshot(),
        other => panic!("{other:?}"),
    };
    assert_eq!(snapshot(), None, "other requests go on, nothing published");
    control(&url, "POST", "release");
    match held.join().unwrap() {
        Exchange::Answered(answer) => assert_eq!(answer.status, 204),
        Exchange::NoAnswer(no) => panic!("{no:?}"),
    }
    assert!(snapshot().is_some(), "published");
    assert_eq!(control(&url, "GET", "held"), "0");
}

// A header read gets the body's first bytes alone whether the server honors `Range` (`206`) or
// answers `200` with the whole body: the transport reads no more and drops the connection. A
// chunk read reads its `206` whole, and the whole body of a `200`.
#[test]
fn a_header_read_takes_only_the_first_bytes_whether_or_not_range_is_honored() {
    let (server, url) = served();
    let http = HttpTransport::new(url);
    let send = |request: &Request| match http.send(request, Attempt::default()) {
        Exchange::Answered(answer) => *answer,
        Exchange::NoAnswer(no) => panic!("{no:?}"),
    };
    assert_eq!(send(&create()).status, 201);
    let token = server.lock().unwrap().token(PATH).unwrap();
    let body = vec![0x5a; 200_000];
    let mut digester = SnapshotDigester::new(CONTENT_TYPE);
    digester.update(&body);
    let publish = Request {
        keys: keys(Some(IncarnationPrecondition { incarnation: token })),
        content_type: Some(CONTENT_TYPE.to_owned()),
        snapshot_digest: Some(digester.finalize()),
        body: Arc::from(body),
        ..Request::new(Method::Put, Target::Snapshot {
            offset: Offset::ZERO,
        })
    };
    assert_eq!(send(&publish).status, 204);
    let ignore_range = || {
        server.lock().unwrap().inject(Scripted {
            method: Some(Method::Get),
            fault: Fault::IgnoreRange,
            times: 1,
        });
    };
    let header = Request::snapshot_read(Offset::ZERO, token, crate::wire::SnapshotPart::Header);
    let statuses: Vec<u16> = (0..4)
        .map(|i| {
            if i % 2 == 1 {
                ignore_range();
            }
            let answer = send(&header);
            assert!(
                matches!(answer.typed, Ok(Typed::SnapshotRead { .. })),
                "{answer:?}"
            );
            assert_eq!(answer.body, vec![0x5a; 56]);
            answer.status
        })
        .collect();
    assert_eq!(statuses, [206, 200, 206, 200]);
    let chunk = Request::snapshot_read(Offset::ZERO, token, crate::wire::SnapshotPart::Chunk {
        first: 65_536,
        last: 131_071,
    });
    let ranged = send(&chunk);
    assert_eq!(ranged.status, 206);
    assert!(matches!(
        ranged.typed,
        Ok(Typed::SnapshotRead { range: Some(span), .. })
            if (span.first(), span.last(), span.total()) == (65_536, 131_071, 200_000)
    ));
    assert_eq!(ranged.body.len(), 65_536);
    ignore_range();
    let whole = send(&chunk);
    assert_eq!(whole.status, 200);
    assert_eq!(whole.body.len(), 200_000);
}

// The header crate end to end: every answer the model renders reads back identically over
// loopback HTTP and in process.
#[test]
fn model_answers_round_trip_through_http() {
    let (over_http, url) = served();
    let http = HttpTransport::new(url);
    let in_process = model();
    let local = ModelTransport::new(Arc::clone(&in_process), PATH);
    let mut statuses = Vec::new();
    for step in 0.. {
        let Some(request) = script(&over_http, step) else {
            break;
        };
        assert_eq!(script(&in_process, step).as_ref(), Some(&request));
        let remote = http.send(&request, Attempt::default());
        let Exchange::Answered(answer) = &remote else {
            panic!("step {step}: {remote:?}");
        };
        if let Err(e) = &answer.typed {
            panic!("step {step}: {e}");
        }
        assert_eq!(
            remote,
            local.send(&request, Attempt::default()),
            "step {step}"
        );
        statuses.push(answer.status);
    }
    assert_eq!(statuses, [
        404, 201, 200, 200, 200, 204, 403, 204, 409, 400, 200, 403, 403, 400, 200, 200, 204, 409,
        206, 204, 412, 413, 200, 409, 409
    ]);
}

#[test]
fn a_refused_connection_sent_nothing() {
    let (addr, _refusing) = refusing();
    let transport = HttpTransport::new(format!("http://{addr}/b/s"));
    let exchange = transport.send(&create(), Attempt::default());
    assert!(
        matches!(exchange, Exchange::NoAnswer(NoAnswer::NotSent { .. })),
        "{exchange:?}"
    );
}

#[test]
fn a_connection_closed_after_the_body_is_lost() {
    let (url, server) = serve(vec![String::new()]);
    let exchange = HttpTransport::new(url).send(&write(None, 1, 0, b"claim"), Attempt {
        fresh_connection: true,
        refresh_token: false,
    });
    assert!(
        matches!(exchange, Exchange::NoAnswer(NoAnswer::Lost { .. })),
        "{exchange:?}"
    );
    server.join().unwrap();
}

// A `401` to the API key's token brings the key's refresh forward: the token no longer counts as
// valid, so a host that uses the key again gets the next one, which requests carry from then on.
// The only test that gives the process a key (requests of other tests running meanwhile carry its
// token), and it forgets the key at the end. The canned answers give only the token answer's
// wire format.
#[test]
fn a_401_to_the_keys_token_refreshes_it() {
    let token = |token: &str| {
        let body = format!(r#"{{"access_token": "{token}", "expires_in": 3600}}"#);
        format!(
            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
    };
    let (endpoint, exchanges) = serve(vec![token("t1"), token("t2")]);
    let wait = Duration::from_secs(30);
    crate::auth::use_key("k-test", &endpoint, Some("streams"), wait).unwrap();

    let (url, refusing) = serve(vec![answer("401 Unauthorized")]);
    let exchange = HttpTransport::new(url).send(&create(), Attempt::default());
    assert!(
        matches!(&exchange, Exchange::Answered(a) if a.status == 401),
        "{exchange:?}"
    );
    let sent = refusing.join().unwrap();
    assert_eq!(request_header(&sent[0], "authorization"), Some("Bearer t1"));

    crate::auth::use_key("k-test", &endpoint, Some("streams"), wait).unwrap();
    assert_eq!(crate::auth::token(false).as_deref(), Some("t2"));
    crate::auth::forget_key();
    let sent = exchanges.join().unwrap();
    for head in &sent {
        assert_eq!(request_header(head, "authorization"), Some("Bearer k-test"));
    }
}

#[test]
fn a_redirect_is_never_followed() {
    let (elsewhere, _refusing) = refusing();
    let location = format!("http://{elsewhere}/b/s");
    let (url, server) = serve(vec![answer(&format!(
        "307 Temporary Redirect\r\nlocation: {location}"
    ))]);
    let exchange = HttpTransport::new(url).send(&write(None, 1, 0, b"claim"), Attempt::default());
    let Exchange::Answered(answered) = exchange else {
        panic!("{exchange:?}");
    };
    assert_eq!(answered.status, 307);
    assert_eq!(answered.typed, Ok(Typed::Untyped));
    assert_eq!(answered.location, Some(location));
    assert_eq!(server.join().unwrap().len(), 1);
}

#[test]
fn every_body_carries_content_length() {
    let (url, server) = serve(vec![
        answer("204 No Content"),
        answer("204 No Content"),
        answer("204 No Content"),
    ]);
    let transport = HttpTransport::new(url);
    let empty_post = Request::new(Method::Post, Target::Stream);
    for request in [create(), empty_post, write(None, 1, 0, b"claim")] {
        let exchange = transport.send(&request, Attempt::default());
        assert!(matches!(exchange, Exchange::Answered(_)), "{exchange:?}");
    }
    let lengths: Vec<Option<String>> = server
        .join()
        .unwrap()
        .iter()
        .map(|head| request_header(head, "content-length").map(str::to_owned))
        .collect();
    assert_eq!(lengths, [
        Some("0".to_owned()),
        Some("0".to_owned()),
        Some("5".to_owned())
    ]);
}

#[test]
fn only_a_proven_unsent_failure_is_not_sent() {
    // rustls reports a bad record on an open connection, one that may have carried the whole
    // request, as `InvalidData`: lost. So is a refusal ureq reports without `net`'s proof.
    for kind in [
        std::io::ErrorKind::InvalidData,
        std::io::ErrorKind::ConnectionRefused,
        std::io::ErrorKind::ConnectionReset,
    ] {
        let lost = no_answer_from(ureq::Error::Io(kind.into()));
        assert!(matches!(lost, NoAnswer::Lost { .. }), "{kind:?}: {lost:?}");
    }
}

#[test]
fn time_limits_scale_with_the_body_and_the_answer() {
    let small = Timeouts::new(0, 0, 256);
    assert_eq!(small.connect, Duration::from_secs(5));
    assert_eq!(small.send_request, Duration::from_secs(5));
    assert_eq!(small.send_body, Duration::from_secs(5));
    assert_eq!(small.recv_response, Duration::from_secs(15));
    assert_eq!(small.recv_body, Duration::from_secs(10));
    // 4 MiB at 256 KiB/s is 16 s; at 1 MiB/s, 4 s.
    let batch = Timeouts::new(4 << 20, 4 << 20, 256);
    assert_eq!(batch.send_body, Duration::from_secs(21));
    assert_eq!(batch.recv_body, Duration::from_secs(26));
    assert_eq!(
        Timeouts::new(4 << 20, 0, 1024).send_body,
        Duration::from_secs(9)
    );
    assert_eq!(Timeouts::new(1024, 0, 0).send_body, Duration::from_secs(6));
    // A commit of 64 parts, up to the 1 GiB snapshot limit: its answer may come 128 s after
    // the 15 s, as a gateway in front of the node waits 128 s after its own lower base.
    let commit = Request {
        parts: Some(Box::new(PartsCommit {
            parts: PartCount::try_from(64_u32).unwrap(),
            digest: SnapshotDigest::from_bytes([9; 32]),
        })),
        ..Request::new(Method::Put, Target::Snapshot {
            offset: Offset::from_u64(1),
        })
    };
    let waiting = Timeouts::new(0, 0, 256).waiting(commit.answer_wait());
    assert_eq!(waiting.recv_response, Duration::from_secs(143));
    assert_eq!(waiting.send_body, Duration::from_secs(5));
}

#[test]
fn the_model_front_types_its_413_with_the_configured_limit() {
    let (server, url) = served();
    server.lock().unwrap().set_limits(Limits {
        max_request_bytes: 3,
        ..Limits::default()
    });
    let exchange = HttpTransport::new(url).send(&write(None, 1, 0, b"four"), Attempt::default());
    let Exchange::Answered(answer) = exchange else {
        panic!("{exchange:?}");
    };
    assert_eq!(answer.status, 413);
    assert_eq!(
        answer.typed,
        Ok(Typed::TooLarge(Some(ursula_headers::SizeLimit::Request(3))))
    );
}

#[test]
fn a_fault_is_consumed_alike_before_a_too_large_body() {
    // The HTTP front refuses an oversized body from `Content-Length`, before it is read; a
    // scripted fault still applies first, as in process.
    let (over_http, url) = served();
    let http = HttpTransport::new(url);
    let in_process = model();
    let local = ModelTransport::new(Arc::clone(&in_process), PATH);
    for server in [&over_http, &in_process] {
        let mut server = server.lock().unwrap();
        server.set_limits(Limits {
            max_request_bytes: 3,
            ..Limits::default()
        });
        server.inject(crate::model::faults::Scripted {
            method: Some(Method::Post),
            fault: crate::model::faults::Fault::Status {
                status: 503,
                retry_after: Some(1),
            },
            times: 1,
        });
    }
    let oversized = write(None, 1, 0, b"too large");
    let mut statuses = Vec::new();
    for _ in 0..2 {
        let remote = http.send(&oversized, Attempt::default());
        assert_eq!(remote, local.send(&oversized, Attempt::default()));
        let Exchange::Answered(answer) = remote else {
            panic!("{remote:?}");
        };
        statuses.push(answer.status);
    }
    assert_eq!(statuses, [503, 413]);
}

// The extensions evaluate a write in one order on every path (Producer Write Answers), so the
// model answers each rule alike over HTTP and in process.
#[test]
fn addendum_4_the_order_is_the_same_over_http_and_in_process() {
    let (over_http, url) = served();
    let http = HttpTransport::new(url);
    let in_process = model();
    let local = ModelTransport::new(Arc::clone(&in_process), PATH);
    let mut statuses = Vec::new();
    for step in 0.. {
        let token = tokened(&over_http);
        assert_eq!(token, tokened(&in_process));
        let request = match step {
            0 => create(),
            1 => write(token, 0, 0, b"claim"),
            2 => write(token, 0, 1, b"b1"),
            // The digest of another body with b1: a key conflict, tokened or not.
            3 => with_body(&write(token, 0, 1, b"b2"), b"b1"),
            4 => with_body(&write(None, 0, 1, b"b2"), b"b1"),
            // A repeat without a content type, and an empty repeat.
            5 => Request {
                content_type: None,
                ..write(token, 0, 1, b"b1")
            },
            6 => with_body(&write(token, 0, 1, b"b1"), b""),
            // Writes that would apply meet the content checks.
            7 => write(token, 0, 2, b""),
            8 => Request {
                content_type: Some("text/plain".to_owned()),
                ..write(token, 0, 2, b"b3")
            },
            9 => with_body(&write(token, 0, 2, b"b3"), b"b4"),
            10 => Request {
                closes: true,
                ..write(token, 0, 2, b"b3")
            },
            11 | 12 => close_only(token, 0, 2),
            13 => close_only(token, 0, 3),
            // Steps 5 and 6 without the token.
            14 => Request {
                content_type: None,
                ..write(None, 0, 1, b"b1")
            },
            15 => with_body(&write(None, 0, 1, b"b1"), b""),
            _ => break,
        };
        let remote = http.send(&request, Attempt::default());
        assert_eq!(
            remote,
            local.send(&request, Attempt::default()),
            "step {step}"
        );
        let Exchange::Answered(answer) = remote else {
            panic!("step {step}: {remote:?}");
        };
        statuses.push(answer.status);
    }
    assert_eq!(statuses, [
        201, 200, 200, 403, 403, 204, 204, 400, 409, 400, 501, 200, 204, 409, 204, 204
    ]);
}

#[test]
fn the_model_front_answers_expect_100_continue() {
    use std::io::BufRead;
    use std::io::BufReader;
    use std::io::Read;
    use std::io::Write;

    let (_server, url) = served();
    let http = HttpTransport::new(url.clone());
    assert!(matches!(
        http.send(&create(), Attempt::default()),
        Exchange::Answered(_)
    ));
    let addr = url
        .strip_prefix("http://")
        .and_then(|rest| rest.strip_suffix(PATH))
        .unwrap();
    let mut connection = std::net::TcpStream::connect(addr).unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let body = b"frame";
    write!(
        connection,
        "POST {PATH} HTTP/1.1\r\nhost: {addr}\r\ncontent-type: {CONTENT_TYPE}\r\n\
         content-length: {}\r\nexpect: 100-continue\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    let mut reader = BufReader::new(connection.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert_eq!(line, "HTTP/1.1 100 Continue\r\n");
    line.clear();
    reader.read_line(&mut line).unwrap();
    assert_eq!(line, "\r\n");
    connection.write_all(body).unwrap();
    let mut rest = String::new();
    reader.read_to_string(&mut rest).unwrap();
    assert!(rest.starts_with("HTTP/1.1 204 "), "{rest}");
}
