//! The model server over HTTP/1.1 with keep-alive, on `std::net`, one thread per connection.
//!
//! Stream requests are read into [`Request`]s with the shared `wire` code (producer headers
//! through `ursula_headers`) and answered by [`ModelServer::exchange`]. A `PUT` or `POST` must
//! carry `Content-Length` (`411` otherwise, chunked bodies are refused with `501`), as the
//! client's transport always sends it.
//!
//! Control routes, under `/__model/` (query values are taken verbatim, not percent-decoded):
//!
//! - `GET /__model/health`: `200`.
//! - `POST /__model/clock?advance_ms=<ms>`: moves the clock; answers the new time.
//! - `POST /__model/checkpoint`: saves every stream; answers the checkpoint id.
//! - `POST /__model/history[?restore=<id>]`: a history event, restoring a checkpoint if named.
//! - `POST /__model/faults[?times=<n>][&method=<METHOD>]`, body one fault line (`faults`).
//! - `PUT /__model/limits?max_request_bytes=<n>&max_snapshot_bytes=<n|cold|none>` (`cold`: a cold store's 1 GiB).
//!   `|none>`.
//! - `POST /__model/foreign?path=<stream path>`, body: an unkeyed append by another writer.
//! - `POST /__model/claim?path=<stream path>&epoch=<e>`, body: a claim by another claimant.
//! - `POST /__model/close?path=<stream path>`, `POST /__model/delete?path=<stream path>`.
//! - `GET /__model/frames?path=<stream path>`: every byte of the stream.

use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use ursula_headers::ClusterId;
use ursula_headers::Epoch;
use ursula_headers::HeaderError;

use super::CheckpointId;
use super::Delivered;
use super::HistoryEvent;
use super::Limits;
use super::ModelError;
use super::ModelServer;
use super::faults::Fault;
use super::faults::FaultSyntax;
use super::faults::Scripted;
use crate::wire::Method;
use crate::wire::RawAnswer;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::WireError;

/// The largest request head read.
const MAX_HEAD_BYTES: usize = 64 << 10;

/// The largest request body read; a larger one closes the connection after its answer.
const MAX_BODY_BYTES: u64 = 64 << 20;

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
        let incoming = match read_request(&mut reader)? {
            Ok(Some(incoming)) => incoming,
            Ok(None) => return Ok(()),
            Err(answer) => {
                write_answer(&mut writer, &answer, false, true)?;
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
/// `Err(answer)` for one that cannot be served (the connection closes after the answer).
fn read_request(reader: &mut impl BufRead) -> io::Result<Result<Option<Incoming>, RawAnswer>> {
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
            return Ok(Err(RawAnswer::new(431)));
        }
    }
    let mut lines = head.iter().map(|line| trim_line(line));
    let request_line = String::from_utf8_lossy(lines.next().unwrap_or_default()).into_owned();
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target), Some(_version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Ok(Err(RawAnswer::new(400)));
    };
    let mut headers = Vec::new();
    for line in lines {
        let Some(colon) = line.iter().position(|b| *b == b':') else {
            return Ok(Err(RawAnswer::new(400)));
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
    if header("transfer-encoding").is_some() {
        return Ok(Err(RawAnswer::new(501)));
    }
    let length = match header("content-length").map(|v| v.parse::<u64>()) {
        None if matches!(method, "PUT" | "POST") => return Ok(Err(RawAnswer::new(411))),
        None => 0,
        Some(Ok(length)) if length <= MAX_BODY_BYTES => length,
        Some(_) => return Ok(Err(RawAnswer::new(413))),
    };
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
        return answer(RawAnswer::new(405));
    };
    if let Some(route) = incoming.target.strip_prefix("/__model/") {
        return answer(control(method, route, &incoming.body, server));
    }
    let request = Target::split(method, &incoming.target).and_then(|(path, target)| {
        let request = Request::parse(
            method,
            target,
            incoming
                .headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_slice())),
            incoming.body.clone(),
        )?;
        Ok((path, request))
    });
    let (path, request) = match request {
        Ok(request) => request,
        Err(e) => return answer(text(400, &e.to_string())),
    };
    let delivered = lock(server).exchange(path, &request);
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
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Status",
    }
}
