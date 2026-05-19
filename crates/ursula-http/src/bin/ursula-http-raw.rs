use std::net::SocketAddr;

use axum::body::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use ursula_runtime::{
    AppendBatchRequest, CreateStreamRequest, RuntimeConfig, RuntimeError, ShardRuntime,
};
use ursula_shard::BucketStreamId;

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse()?;
    let runtime = ShardRuntime::spawn(RuntimeConfig::new(args.core_count, args.raft_group_count))?;
    let listener = TcpListener::bind(args.listen).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let _ = serve_connection(stream, runtime).await;
        });
    }
}

async fn serve_connection(mut stream: TcpStream, runtime: ShardRuntime) -> std::io::Result<()> {
    let mut buffer = Vec::with_capacity(8192);
    let mut scratch = [0u8; 8192];
    loop {
        let Some(request) = read_request(&mut stream, &mut buffer, &mut scratch).await? else {
            return Ok(());
        };
        let close = request.connection_close;
        let response = handle_request(request, &runtime).await;
        stream.write_all(&response).await?;
        if close {
            return Ok(());
        }
    }
}

async fn read_request(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
    scratch: &mut [u8],
) -> std::io::Result<Option<Request>> {
    loop {
        if let Some(header_end) = find_header_end(buffer) {
            let parsed = parse_headers(&buffer[..header_end]);
            let Ok((method, path, content_length, content_type, minimal_ack, connection_close)) =
                parsed
            else {
                drain_request(buffer, header_end, 0);
                return Ok(Some(Request::bad_request("bad request")));
            };
            if content_length > MAX_BODY_BYTES {
                drain_request(buffer, header_end, 0);
                return Ok(Some(Request::bad_request("request body is too large")));
            }
            let request_end = header_end + content_length;
            while buffer.len() < request_end {
                let read = stream.read(scratch).await?;
                if read == 0 {
                    return Ok(None);
                }
                buffer.extend_from_slice(&scratch[..read]);
            }
            let body = buffer[header_end..request_end].to_vec();
            buffer.drain(..request_end);
            return Ok(Some(Request {
                method,
                path,
                content_type,
                minimal_ack,
                connection_close,
                body,
                prebuilt_error: None,
            }));
        }
        if buffer.len() > MAX_HEADER_BYTES {
            buffer.clear();
            return Ok(Some(Request::bad_request("request headers are too large")));
        }
        let read = stream.read(scratch).await?;
        if read == 0 {
            return Ok(None);
        }
        buffer.extend_from_slice(&scratch[..read]);
    }
}

fn drain_request(buffer: &mut Vec<u8>, header_end: usize, content_length: usize) {
    let request_end = header_end.saturating_add(content_length).min(buffer.len());
    buffer.drain(..request_end);
}

async fn handle_request(request: Request, runtime: &ShardRuntime) -> Vec<u8> {
    if let Some(message) = request.prebuilt_error {
        return plain_response(400, message.as_bytes());
    }

    let parts = request
        .path
        .split('?')
        .next()
        .unwrap_or_default()
        .trim_matches('/')
        .split('/')
        .collect::<Vec<_>>();

    match (request.method.as_str(), parts.as_slice()) {
        ("PUT", ["benchcmp"]) => empty_response(201),
        ("PUT", ["benchcmp", stream]) => {
            let stream_id = BucketStreamId::new("benchcmp", *stream);
            let mut create = CreateStreamRequest::new(stream_id, request.content_type);
            create.initial_payload = Bytes::from(request.body);
            match runtime.create_stream(create).await {
                Ok(response) => empty_response(if response.already_exists { 200 } else { 201 }),
                Err(err) => runtime_error_response(err),
            }
        }
        ("POST", ["benchcmp", stream, "append-batch"]) => {
            let body = Bytes::from(request.body);
            let payloads = match parse_append_batch(&body) {
                Ok(payloads) => payloads,
                Err(message) => return plain_response(400, message.as_bytes()),
            };
            let mut append =
                AppendBatchRequest::new(BucketStreamId::new("benchcmp", *stream), payloads);
            append.content_type = request.content_type;
            let response = match runtime.append_batch(append).await {
                Ok(response) => response,
                Err(err) => return runtime_error_response(err),
            };
            if request.minimal_ack && response.items.iter().all(Result::is_ok) {
                return empty_response(204);
            }
            json_response(200, render_batch_results(&response.items).as_bytes())
        }
        _ => plain_response(404, b"not found"),
    }
}

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    content_type: String,
    minimal_ack: bool,
    connection_close: bool,
    body: Vec<u8>,
    prebuilt_error: Option<String>,
}

impl Request {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            method: String::new(),
            path: String::new(),
            content_type: DEFAULT_CONTENT_TYPE.to_owned(),
            minimal_ack: false,
            connection_close: true,
            body: Vec::new(),
            prebuilt_error: Some(message.into()),
        }
    }
}

fn parse_headers(
    headers: &[u8],
) -> Result<(String, String, usize, String, bool, bool), std::str::Utf8Error> {
    let raw = std::str::from_utf8(headers)?;
    let mut lines = raw.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default().to_owned();
    let path = request_line.next().unwrap_or_default().to_owned();
    let mut content_length = 0usize;
    let mut content_type = DEFAULT_CONTENT_TYPE.to_owned();
    let mut minimal_ack = false;
    let mut connection_close = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().unwrap_or(usize::MAX);
        } else if name.eq_ignore_ascii_case("content-type") && !value.is_empty() {
            content_type = value.to_owned();
        } else if name.eq_ignore_ascii_case("prefer") {
            minimal_ack = value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("return=minimal"));
        } else if name.eq_ignore_ascii_case("connection") {
            connection_close = value.eq_ignore_ascii_case("close");
        }
    }
    Ok((
        method,
        path,
        content_length,
        content_type,
        minimal_ack,
        connection_close,
    ))
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn parse_append_batch(body: &Bytes) -> Result<Vec<Bytes>, String> {
    let mut payloads = Vec::new();
    let mut cursor = 0usize;
    while cursor < body.len() {
        let Some(header_end) = cursor.checked_add(4) else {
            return Err("append batch frame offset overflow".to_owned());
        };
        if header_end > body.len() {
            return Err("append batch frame is missing length header".to_owned());
        }
        let len = u32::from_be_bytes(
            body[cursor..header_end]
                .try_into()
                .expect("slice length is exactly 4"),
        ) as usize;
        cursor = header_end;
        let Some(payload_end) = cursor.checked_add(len) else {
            return Err("append batch payload length overflow".to_owned());
        };
        if payload_end > body.len() {
            return Err("append batch frame payload is truncated".to_owned());
        }
        payloads.push(body.slice(cursor..payload_end));
        cursor = payload_end;
    }
    if payloads.is_empty() {
        return Err("append batch must contain at least one frame".to_owned());
    }
    Ok(payloads)
}

fn render_batch_results(
    results: &[Result<ursula_runtime::AppendResponse, RuntimeError>],
) -> String {
    const OK_ACK: &str = "{\"status\":204}";
    if results.iter().all(Result::is_ok) {
        let mut body = String::with_capacity(2 + results.len().saturating_mul(OK_ACK.len() + 1));
        body.push('[');
        for index in 0..results.len() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(OK_ACK);
        }
        body.push(']');
        return body;
    }

    let mut body = String::with_capacity(2 + results.len().saturating_mul(OK_ACK.len() + 1));
    body.push('[');
    for (index, result) in results.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        let status = match result {
            Ok(_) => 204,
            Err(err) => runtime_error_status(err),
        };
        body.push_str("{\"status\":");
        body.push_str(&status.to_string());
        body.push('}');
    }
    body.push(']');
    body
}

fn runtime_error_response(err: RuntimeError) -> Vec<u8> {
    let body = err.to_string();
    plain_response(runtime_error_status(&err), body.as_bytes())
}

fn runtime_error_status(err: &RuntimeError) -> u16 {
    match err {
        RuntimeError::EmptyAppend
        | RuntimeError::InvalidRaftGroup { .. }
        | RuntimeError::SnapshotPlacementMismatch { .. } => 400,
        RuntimeError::LiveReadBackpressure { .. } => 503,
        RuntimeError::GroupEngine { message, .. } => stream_error_status(message),
        RuntimeError::InvalidConfig(_)
        | RuntimeError::ColdStoreConfig { .. }
        | RuntimeError::ColdStoreIo { .. }
        | RuntimeError::MailboxClosed { .. }
        | RuntimeError::ResponseDropped { .. }
        | RuntimeError::SpawnCoreThread { .. } => 500,
    }
}

fn stream_error_status(message: &str) -> u16 {
    if message.contains("NotFound") {
        404
    } else if message.contains("ContentTypeMismatch")
        || message.contains("StreamAlreadyExistsConflict")
        || message.contains("StreamClosed")
        || message.contains("StreamSeqConflict")
        || message.contains("ProducerSeqConflict")
    {
        409
    } else if message.contains("ProducerEpochStale") {
        403
    } else if message.contains("Invalid") || message.contains("EmptyAppend") {
        400
    } else if message.contains("OffsetOutOfRange") {
        416
    } else {
        500
    }
}

fn empty_response(status: u16) -> Vec<u8> {
    response(status, None, &[])
}

fn json_response(status: u16, body: &[u8]) -> Vec<u8> {
    response(status, Some("application/json"), body)
}

fn plain_response(status: u16, body: &[u8]) -> Vec<u8> {
    response(status, Some("text/plain; charset=utf-8"), body)
}

fn response(status: u16, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    let reason = reason_phrase(status);
    let mut response = Vec::with_capacity(128 + body.len());
    response.extend_from_slice(format!("HTTP/1.1 {status} {reason}\r\n").as_bytes());
    response.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    response.extend_from_slice(b"Connection: keep-alive\r\n");
    if let Some(content_type) = content_type {
        response.extend_from_slice(format!("Content-Type: {content_type}\r\n").as_bytes());
    }
    response.extend_from_slice(b"\r\n");
    response.extend_from_slice(body);
    response
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        416 => "Range Not Satisfiable",
        _ => "Internal Server Error",
    }
}

#[derive(Debug)]
struct Args {
    listen: SocketAddr,
    core_count: usize,
    raft_group_count: usize,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut listen = "127.0.0.1:4437"
            .parse::<SocketAddr>()
            .expect("default listen addr is valid");
        let mut core_count = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);
        let mut raft_group_count = core_count.saturating_mul(16).max(1);

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--listen" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--listen requires an address".to_owned())?;
                    listen = raw
                        .parse()
                        .map_err(|err| format!("invalid --listen address '{raw}': {err}"))?;
                }
                "--core-count" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--core-count requires a value".to_owned())?;
                    core_count = raw
                        .parse()
                        .map_err(|err| format!("invalid --core-count '{raw}': {err}"))?;
                }
                "--raft-group-count" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--raft-group-count requires a value".to_owned())?;
                    raft_group_count = raw
                        .parse()
                        .map_err(|err| format!("invalid --raft-group-count '{raw}': {err}"))?;
                }
                "--help" | "-h" => return Err(help()),
                other => return Err(format!("unknown argument '{other}'\n\n{}", help())),
            }
        }

        Ok(Self {
            listen,
            core_count,
            raft_group_count,
        })
    }
}

fn help() -> String {
    "usage: ursula-http-raw [--listen ADDR] [--core-count N] [--raft-group-count N]".to_owned()
}
