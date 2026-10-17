//! The model server over HTTP/1.1 with keep-alive, on `std::net`, one thread per connection.
//!
//! Stream requests are read into [`Request`]s with the shared `wire` code (producer headers
//! through `ursula_headers`) and answered by [`ModelServer::exchange`]. A `PUT` or `POST` must
//! carry `Content-Length` (`411` otherwise, chunked bodies are refused with `501`), as the
//! client's transport always sends it. A stream request whose `Content-Length` exceeds the
//! model's request limit (`/__model/limits`) gets the typed `413` (or a scripted fault) decided
//! before its body is read, which is then read and dropped, as the server does on HTTP/1.1
//! (extensions: Answers Before the Body),
//! before the answer goes out and the connection closes; a client that sent
//! `Expect: 100-continue` sends no body. Otherwise `Expect: 100-continue` gets `100 Continue`,
//! then the body is read: the model
//! answers every write after its body, in the order of #581 addendum 4, which an early answer
//! follows too, so the answer is the same.
//!
//! Every refusal carries the server's error body (extensions §11), but those of a head the model
//! cannot read (`400`, `431`), which hyper sends bare before the server's handlers run.
//!
//! Control routes, under `/__model/` (query values are taken verbatim, not percent-decoded):
//!
//! - `GET /__model/health`: `200`.
//! - `POST /__model/clock?advance_ms=<ms>`: moves the clock; answers the new time.
//! - `POST /__model/checkpoint`: saves every stream; answers the checkpoint id.
//! - `POST /__model/history[?restore=<id>]`: a history event, restoring a checkpoint if named.
//! - `POST /__model/faults[?times=<n>][&method=<METHOD>]`, body one fault line (`faults`).
//! - `PUT /__model/limits?max_request_bytes=<n>&max_snapshot_bytes=<n|cold|none>`, where `cold`
//!   stands for a cold store's 1 GiB.
//! - `POST /__model/foreign?path=<stream path>`, body: an unkeyed append by another writer.
//! - `POST /__model/claim?path=<stream path>&epoch=<e>`, body: a claim by another claimant.
//! - `POST /__model/close?path=<stream path>`, `POST /__model/delete?path=<stream path>`.
//! - `GET /__model/frames?path=<stream path>`: every byte of the stream.
//! - `POST /__model/hold`: holds every snapshot `PUT` (a publish or a parts commit) before the
//!   model sees it, until `POST /__model/release`; `GET /__model/held` answers how many are held.

use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use ursula_headers::ClusterId;
use ursula_headers::Epoch;
use ursula_headers::ErrorBody;
use ursula_headers::ErrorCode;
use ursula_headers::HeaderError;
use ursula_headers::PartsCommit;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::SizeLimit;

use super::CheckpointId;
use super::Delivered;
use super::HistoryEvent;
use super::Limits;
use super::ModelError;
use super::ModelServer;
use super::faults::Fault;
use super::faults::FaultSyntax;
use super::faults::Scripted;
use super::refusal;
use super::render_error;
use super::size_refusal;
use crate::wire::Method;
use crate::wire::RawAnswer;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::WireError;

/// The largest request head read.
const MAX_HEAD_BYTES: usize = 64 << 10;

/// The largest body of a control request read; a larger one closes the connection after its
/// answer.
const MAX_CONTROL_BODY_BYTES: u64 = 64 << 20;

/// The largest body of a request refused before its body that is read and dropped before the
/// answer; a larger one is answered unread.
const MAX_DRAIN_BYTES: u64 = 2 << 30;

/// Signalled on every release of held requests (`/__model/release`), paired with the model's
/// mutex. One for the process: a waiter whose model still holds waits again.
static RELEASED: Condvar = Condvar::new();

/// Why the model server stopped.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("invalid cluster id: {0}")]
    ClusterId(#[from] HeaderError),
    #[error("accepting a connection failed: {0}")]
    Accept(#[from] io::Error),
}

/// Serves a fresh model of the deployment `cluster_id` on `listener` until accepting fails.
/// `seed` draws its history ids.
pub fn serve_forever(listener: TcpListener, cluster_id: &str, seed: u64) -> Result<(), ServeError> {
    let server = ModelServer::new(ClusterId::new(cluster_id)?, seed);
    serve(listener, Arc::new(Mutex::new(server)))?;
    Ok(())
}

/// Serves `server` on `listener`, one thread per connection, until accepting fails.
pub(crate) fn serve(listener: TcpListener, server: Arc<Mutex<ModelServer>>) -> io::Result<()> {
    loop {
        let (connection, _peer) = listener.accept()?;
        let server = Arc::clone(&server);
        // Each connection's thread ends when its client closes it: nothing joins it.
        // A connection that breaks is the client's business: the model keeps serving the others.
        drop(std::thread::spawn(move || {
            if let Err(_broken) = connection_loop(connection, &server) {}
        }));
    }
}

fn lock(server: &Mutex<ModelServer>) -> std::sync::MutexGuard<'_, ModelServer> {
    server.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A request as read off the connection.
struct Incoming {
    method: String,
    target: String,
    headers: Vec<(String, Vec<u8>)>,
    body: Vec<u8>,
    close: bool,
}

/// What to do with one request.
enum Reply {
    Answer {
        answer: RawAnswer,
        head_only: bool,
    },
    /// Close the connection without an answer.
    Hang,
}

fn connection_loop(connection: TcpStream, server: &Mutex<ModelServer>) -> io::Result<()> {
    let mut reader = BufReader::new(connection.try_clone()?);
    let mut writer = connection;
    loop {
        let incoming = match read_request(&mut reader, &mut writer, server)? {
            Ok(Some(incoming)) => incoming,
            Ok(None) => return Ok(()),
            // Refused before the body: the connection closes after the answer.
            Err(Reply::Hang) => return Ok(()),
            Err(Reply::Answer { answer, head_only }) => {
                write_answer(&mut writer, &answer, head_only, true)?;
                return Ok(());
            }
        };
        let close = incoming.close;
        let reply = reply(&incoming, server);
        match reply {
            Reply::Hang => return Ok(()),
            Reply::Answer { answer, head_only } => {
                write_answer(&mut writer, &answer, head_only, close)?;
            }
        }
        if close {
            return Ok(());
        }
    }
}

/// Reads one request: `Ok(None)` when the client closed the connection between requests,
/// `Err(reply)` for one refused before its body (the connection closes after the reply).
fn read_request(
    reader: &mut impl BufRead,
    writer: &mut impl Write,
    server: &Mutex<ModelServer>,
) -> io::Result<Result<Option<Incoming>, Reply>> {
    let mut head = Vec::new();
    loop {
        let mut line = Vec::new();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 {
            return if head.is_empty() {
                Ok(Ok(None))
            } else {
                Err(io::ErrorKind::UnexpectedEof.into())
            };
        }
        if line == b"\r\n" || line == b"\n" {
            break;
        }
        head.push(line);
        if head.iter().map(Vec::len).sum::<usize>() > MAX_HEAD_BYTES {
            return Ok(Err(answer(RawAnswer::new(431))));
        }
    }
    let mut lines = head.iter().map(|line| trim_line(line));
    let request_line = String::from_utf8_lossy(lines.next().unwrap_or_default()).into_owned();
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target), Some(_version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Ok(Err(answer(RawAnswer::new(400))));
    };
    let mut headers = Vec::new();
    for line in lines {
        let Some(colon) = line.iter().position(|b| *b == b':') else {
            return Ok(Err(answer(RawAnswer::new(400))));
        };
        let (name, value) = line.split_at(colon);
        let value = value.get(1..).unwrap_or_default().trim_ascii();
        headers.push((
            String::from_utf8_lossy(name).trim().to_owned(),
            value.to_vec(),
        ));
    }
    let header = |wanted: &str| {
        headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(wanted))
            .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
    };
    let close = header("connection").is_some_and(|v| v.eq_ignore_ascii_case("close"));
    // Not modeled: the code the server's net gives a `5xx` that no handler rendered.
    if header("transfer-encoding").is_some() {
        return Ok(Err(answer(refusal(501, ErrorCode::Internal))));
    }
    let control = target.starts_with("/__model/");
    let limit = if control {
        MAX_CONTROL_BODY_BYTES
    } else {
        lock(server).limits().max_request_bytes
    };
    let length = match header("content-length").map(|v| v.parse::<u64>()) {
        // The model's own check: hyper reads a `PUT` or `POST` without a length as an empty
        // body, so the server never answers `411`.
        None if matches!(method, "PUT" | "POST") => {
            return Ok(Err(answer(refusal(411, ErrorCode::InvalidRequest))));
        }
        None => 0,
        Some(Ok(length)) if length <= limit => length,
        // Decided from `Content-Length`, before the body (extensions: Server Metadata), through the
        // same faults as in process.
        Some(Ok(length)) => {
            // As the server's early answers on HTTP/1.1 (extensions: Answers Before the Body): the
            // body is read and dropped first, within the drain bound, so a client still writing it
            // never meets a closed socket (which macOS reports as `EINVAL` rather than the answer).
            // A client that sent `Expect: 100-continue` gets no `100 Continue` and sends no body.
            let expects_continue =
                header("expect").is_some_and(|v| v.eq_ignore_ascii_case("100-continue"));
            if !expects_continue && length <= MAX_DRAIN_BYTES {
                io::copy(&mut reader.by_ref().take(length), &mut io::sink())?;
            }
            let refused = match Method::parse(method) {
                Some(method) if !control => {
                    delivered(lock(server).exchange_too_large(method), method)
                }
                _ => answer(size_refusal(SizeLimit::Request(limit))),
            };
            return Ok(Err(refused));
        }
        Some(Err(_not_a_length)) => return Ok(Err(answer(RawAnswer::new(400)))),
    };
    // A client that sent `Expect: 100-continue` waits for this before it sends the body.
    if length > 0 && header("expect").is_some_and(|v| v.eq_ignore_ascii_case("100-continue")) {
        writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
        writer.flush()?;
    }
    let mut body = Vec::new();
    reader.take(length).read_to_end(&mut body)?;
    if u64::try_from(body.len()).ok() != Some(length) {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(Ok(Some(Incoming {
        method: method.to_owned(),
        target: target.to_owned(),
        headers,
        body,
        close,
    })))
}

fn trim_line(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn reply(incoming: &Incoming, server: &Mutex<ModelServer>) -> Reply {
    let Some(method) = Method::parse(&incoming.method) else {
        return answer(refusal(405, ErrorCode::MethodNotAllowed));
    };
    if let Some(route) = incoming.target.strip_prefix("/__model/") {
        return answer(control(method, route, &incoming.body, server));
    }
    let (path, target) = match Target::split(method, &incoming.target) {
        Ok(split) => split,
        // An offset in the path, or the read's query. The server ignores an unknown query
        // parameter: refusing one is the model's own check.
        Err(e) => {
            let code = match e {
                WireError::Header(_) => ErrorCode::InvalidOffset,
                WireError::Query(_) => ErrorCode::InvalidQuery,
            };
            return answer(malformed(code, &e));
        }
    };
    let headers = || {
        incoming
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice()))
    };
    let request = match Request::parse(method, target, headers(), incoming.body.clone()) {
        Ok(request) => request,
        Err(e) => return answer(malformed(malformed_code(method, target, headers()), &e)),
    };
    let mut model = lock(server);
    if model.holds(&request) {
        model = RELEASED
            .wait_while(model, |model| model.holding())
            .unwrap_or_else(PoisonError::into_inner);
    }
    let exchanged = model.exchange(path, &request);
    drop(model);
    delivered(exchanged, method)
}

/// The reply to a request the server or a scripted fault decided.
fn delivered(delivered: Delivered, method: Method) -> Reply {
    match delivered {
        Delivered::Answered { answer, delay_ms } => {
            if delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
            Reply::Answer {
                answer,
                head_only: method == Method::Head,
            }
        }
        Delivered::Unreachable | Delivered::DroppedBeforeApply | Delivered::DroppedAfterApply => {
            Reply::Hang
        }
    }
}

fn answer(answer: RawAnswer) -> Reply {
    Reply::Answer {
        answer,
        head_only: false,
    }
}

/// The code the server gives a request whose headers [`Request::parse`] refused, by route and by
/// the header that failed: the producer headers and `Stream-Incarnation` of an append or a create
/// are the producer key (`INVALID_PRODUCER`), a malformed `Stream-Snapshot-Parts` is a malformed
/// commit (`INVALID_PARTS_COMMIT`), and any other header, the `Stream-Incarnation` precondition of
/// the other routes among them, is `INVALID_HEADER`.
fn malformed_code<'a, I>(method: Method, target: Target, headers: I) -> ErrorCode
where I: IntoIterator<Item = (&'a str, &'a [u8])> + Clone {
    let keyed_route = target == Target::Stream && matches!(method, Method::Post | Method::Put);
    match (
        ProducerRequestHeaders::parse(headers.clone()),
        PartsCommit::parse(headers),
    ) {
        (Err(_), _) if keyed_route => ErrorCode::InvalidProducer,
        (Ok(_), Err(_)) => ErrorCode::InvalidPartsCommit,
        _ => ErrorCode::InvalidHeader,
    }
}

/// The `400` of a malformed request, with the error body of `code` naming why.
fn malformed(code: ErrorCode, why: &WireError) -> RawAnswer {
    let mut answer = RawAnswer::new(400);
    render_error(&mut answer, &ErrorBody::new(code, why.to_string()));
    answer
}

fn text(status: u16, body: &str) -> RawAnswer {
    let mut answer = RawAnswer::new(status);
    answer.body = body.as_bytes().to_vec();
    answer
}

/// A control route's failure.
#[derive(Debug, thiserror::Error)]
enum ControlError {
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(transparent)]
    Fault(#[from] FaultSyntax),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error("missing or malformed query parameter `{0}`")]
    Query(&'static str),
    #[error("no control route {0}")]
    Route(String),
}

fn control(method: Method, route: &str, body: &[u8], server: &Mutex<ModelServer>) -> RawAnswer {
    match run_control(method, route, body, server) {
        Ok(answer) => answer,
        Err(ControlError::Route(route)) => text(404, &route),
        Err(e) => text(400, &e.to_string()),
    }
}

fn run_control(
    method: Method,
    route: &str,
    body: &[u8],
    server: &Mutex<ModelServer>,
) -> Result<RawAnswer, ControlError> {
    let (name, query) = route.split_once('?').unwrap_or((route, ""));
    let param = |key: &str| {
        query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find_map(|(k, v)| (k == key).then_some(v))
    };
    let number = |key: &'static str| -> Result<u64, ControlError> {
        param(key)
            .and_then(|v| v.parse().ok())
            .ok_or(ControlError::Query(key))
    };
    let path = || param("path").ok_or(ControlError::Query("path"));
    let mut server = lock(server);
    let answer = match (method, name) {
        (Method::Get, "health") => RawAnswer::new(200),
        (Method::Post, "clock") => {
            server.advance(number("advance_ms")?);
            text(200, &server.now_ms().to_string())
        }
        (Method::Post, "checkpoint") => text(200, &server.checkpoint().0.to_string()),
        (Method::Post, "history") => {
            let event = match param("restore") {
                None => HistoryEvent::NewHistory,
                Some(_) => HistoryEvent::Restore(CheckpointId(
                    usize::try_from(number("restore")?)
                        .map_err(|_overflow| ControlError::Query("restore"))?,
                )),
            };
            server.history_event(event)?;
            RawAnswer::new(204)
        }
        (Method::Post, "faults") => {
            let method = match param("method") {
                None => None,
                Some(m) => Some(Method::parse(m).ok_or(ControlError::Query("method"))?),
            };
            let times = match param("times") {
                None => 1,
                Some(_) => u32::try_from(number("times")?)
                    .map_err(|_overflow| ControlError::Query("times"))?,
            };
            let fault = Fault::parse(&String::from_utf8_lossy(body))?;
            server.inject(Scripted {
                method,
                fault,
                times,
            });
            RawAnswer::new(204)
        }
        (Method::Put, "limits") => {
            let mut limits = Limits {
                max_request_bytes: number("max_request_bytes")?,
                ..server.limits()
            };
            limits.max_snapshot_bytes = match param("max_snapshot_bytes") {
                None | Some("none") => None,
                Some("cold") => Some(super::MAX_SNAPSHOT_BYTES),
                Some(_) => Some(number("max_snapshot_bytes")?),
            };
            server.set_limits(limits);
            RawAnswer::new(204)
        }
        (Method::Post, "foreign") => {
            let tail = server.foreign_append(path()?, body)?;
            text(200, &tail.to_string())
        }
        (Method::Post, "claim") => {
            let epoch = Epoch::try_from(number("epoch")?)
                .map_err(|e| ControlError::Wire(WireError::Header(e)))?;
            server.foreign_claim(path()?, epoch, body)?
        }
        (Method::Post, "hold") => {
            server.hold_snapshots();
            RawAnswer::new(204)
        }
        (Method::Post, "release") => {
            server.release();
            RELEASED.notify_all();
            RawAnswer::new(204)
        }
        (Method::Get, "held") => text(200, &server.held().to_string()),
        (Method::Post, "close") => {
            server.close(path()?)?;
            RawAnswer::new(204)
        }
        (Method::Post, "delete") => {
            server.delete(path()?)?;
            RawAnswer::new(204)
        }
        (Method::Get, "frames") => {
            let path = path()?;
            let bytes = server
                .frames(path)
                .ok_or_else(|| ModelError::NoStream(path.to_owned()))?;
            let mut answer = RawAnswer::new(200);
            answer.body = bytes.to_vec();
            answer
        }
        _ => return Err(ControlError::Route(route.to_owned())),
    };
    Ok(answer)
}

fn write_answer(
    writer: &mut impl Write,
    answer: &RawAnswer,
    head_only: bool,
    close: bool,
) -> io::Result<()> {
    let mut out = format!("HTTP/1.1 {} {}\r\n", answer.status, reason(answer.status));
    for (name, value) in &answer.headers {
        out.push_str(&format!("{name}: {value}\r\n"));
    }
    // No `Content-Length` on a `204` (RFC 9110 §8.6) or a `HEAD` answer.
    if !head_only && answer.status != 204 {
        out.push_str(&format!("content-length: {}\r\n", answer.body.len()));
    }
    if close {
        out.push_str("connection: close\r\n");
    }
    out.push_str("\r\n");
    writer.write_all(out.as_bytes())?;
    if !head_only {
        writer.write_all(&answer.body)?;
    }
    writer.flush()
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        206 => "Partial Content",
        307 => "Temporary Redirect",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Content Too Large",
        416 => "Range Not Satisfiable",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Status",
    }
}
