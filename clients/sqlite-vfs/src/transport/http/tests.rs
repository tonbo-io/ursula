//! [`HttpTransport`] over loopback: against the model server, and against scripted sockets.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use ursula_headers::ClusterId;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Offset;
use ursula_headers::ProducerRequestHeaders;

use super::HttpTransport;
use super::Timeouts;
use super::no_answer_from;
use crate::client::mock::answer;
use crate::client::mock::refusing;
use crate::client::mock::request_header;
use crate::client::mock::serve;
use crate::codec::digest::SnapshotDigester;
use crate::codec::frame::CONTENT_TYPE;
use crate::model::Limits;
use crate::model::ModelServer;
use crate::model::http;
use crate::model::tests::create;
use crate::model::tests::write;
use crate::transport::Attempt;
use crate::transport::Transport;
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
        18 => Request::snapshot_read(Offset::from_u64(5), 1 << 20),
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
        200, 204, 412, 413, 200, 409, 409
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
