use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::Context;
use anyhow::Result;
use bytes::BytesMut;
use clap::Args;
use futures::StreamExt;
use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::sync::Barrier;
use tokio::sync::Mutex;

use crate::backend::ApiStyle;
use crate::backend::Backend;
use crate::common::Counts;
use crate::common::LatencySummary;
use crate::common::build_client;
use crate::common::merge;
use crate::common::new_histogram;
use crate::common::summarize;

#[derive(Args, Debug, Clone)]
pub struct FanOutArgs {
    #[arg(long)]
    pub target: String,

    #[arg(long, value_enum, default_value_t = ApiStyle::Ursula)]
    pub api_style: ApiStyle,

    #[arg(long, default_value = "bench-fanout")]
    pub bucket: String,

    #[arg(long, default_value = "benchmark")]
    pub basin: String,

    #[arg(long, default_value = "doc")]
    pub stream: String,

    #[arg(long, default_value_t = 100)]
    pub subscribers: usize,

    #[arg(long, default_value_t = 50)]
    pub writer_rate: u64,

    #[arg(long, default_value_t = 30)]
    pub duration_secs: u64,

    #[arg(long, default_value_t = 256)]
    pub payload_bytes: usize,

    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,

    #[arg(long, default_value_t = 15)]
    pub subscriber_idle_timeout_secs: u64,
}

#[derive(Serialize)]
pub struct FanOutResult {
    pub scenario: &'static str,
    pub api_style: ApiStyle,
    pub target: String,
    pub bucket: String,
    pub basin: String,
    pub stream: String,
    pub subscribers: usize,
    pub writer_rate: u64,
    pub duration_secs: u64,
    pub payload_bytes: usize,
    pub elapsed_secs: f64,
    pub events_sent: u64,
    pub events_received: u64,
    pub subscriber_errors: u64,
    pub append_counts: Counts,
    pub fan_out_latency_ms: LatencySummary,
}

pub async fn run(args: FanOutArgs) -> Result<FanOutResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(
        args.api_style,
        &args.target,
        &args.bucket,
        &args.basin,
        client,
    );
    let stream = args.stream.clone();

    backend.ensure_namespace().await?;
    backend.create_stream(&stream, "text/plain").await?;

    let ready_barrier = Arc::new(Barrier::new(args.subscribers + 1));
    let events_received = Arc::new(AtomicU64::new(0));
    let subscriber_errors = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(new_histogram()));
    let deadline = Arc::new(tokio::sync::OnceCell::<Instant>::new());

    let mut subs = Vec::with_capacity(args.subscribers);
    for idx in 0..args.subscribers {
        let backend = backend.clone();
        let stream = stream.clone();
        let barrier = ready_barrier.clone();
        let recv = events_received.clone();
        let serr = subscriber_errors.clone();
        let hist = hist.clone();
        let deadline = deadline.clone();
        let idle = Duration::from_secs(args.subscriber_idle_timeout_secs);
        subs.push(tokio::spawn(async move {
            if let Err(e) =
                run_subscriber(&backend, &stream, idx, barrier, recv, hist, deadline, idle).await
            {
                tracing::warn!("subscriber failed: idx={idx} error={e:#}");
                serr.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let writer_barrier = ready_barrier.clone();
    let writer_payload_size = args.payload_bytes.max(48);
    let writer_rate = args.writer_rate.max(1);
    let writer_duration = args.duration_secs;
    let writer_backend = backend.clone();
    let writer_stream = stream.clone();
    let deadline_setter = deadline.clone();
    let writer = tokio::spawn(async move {
        writer_barrier.wait().await;
        let start = Instant::now();
        let dl = start + Duration::from_secs(writer_duration);
        let _ = deadline_setter.set(dl);
        run_writer(
            &writer_backend,
            &writer_stream,
            writer_rate,
            writer_payload_size,
            dl,
        )
        .await
    });

    let start = Instant::now();
    let (events_sent, append_counts) = writer.await??;
    let elapsed = start.elapsed();

    let drain_limit = Duration::from_secs(args.subscriber_idle_timeout_secs + 5);
    let _ = tokio::time::timeout(drain_limit, futures::future::join_all(subs)).await;

    let hist = hist.lock().await;
    let latency = summarize(&hist);

    Ok(FanOutResult {
        scenario: "fanout",
        api_style: args.api_style,
        target: args.target,
        bucket: args.bucket,
        basin: args.basin,
        stream: args.stream,
        subscribers: args.subscribers,
        writer_rate: args.writer_rate,
        duration_secs: args.duration_secs,
        payload_bytes: args.payload_bytes,
        elapsed_secs: elapsed.as_secs_f64(),
        events_sent,
        events_received: events_received.load(Ordering::Relaxed),
        subscriber_errors: subscriber_errors.load(Ordering::Relaxed),
        append_counts,
        fan_out_latency_ms: latency,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_subscriber(
    backend: &Backend,
    stream: &str,
    _idx: usize,
    barrier: Arc<Barrier>,
    recv: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
    deadline: Arc<tokio::sync::OnceCell<Instant>>,
    idle: Duration,
) -> Result<()> {
    let (url, headers) = backend.sse_url_for(_idx, stream);
    let resp = backend
        .client
        .get(&url)
        .headers(headers)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("SSE open: {} {}", resp.status(), url);
    }

    barrier.wait().await;

    let mut local = new_histogram();
    let result: Result<()> = async {
        let mut stream_body = resp.bytes_stream();
        let mut buf = BytesMut::with_capacity(8192);
        let mut last_event_at = Instant::now();
        loop {
            let dl = deadline.get().copied();
            if let Some(end) = dl
                && Instant::now() >= end + Duration::from_secs(2)
            {
                break;
            }
            let to = match dl {
                Some(end) => end.saturating_duration_since(Instant::now()) + Duration::from_secs(2),
                None => idle,
            }
            .min(idle);
            let next = tokio::time::timeout(to, stream_body.next()).await;
            let chunk = match next {
                Ok(Some(Ok(c))) => c,
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => {
                    if last_event_at.elapsed() > idle {
                        break;
                    }
                    continue;
                }
            };
            buf.extend_from_slice(&chunk);
            while let Some(idx) = find_event_end(&buf) {
                let raw = buf.split_to(idx + 2).freeze();
                if let Some(payload) = parse_sse_data(&raw)
                    && let Some(sent_ns) = extract_send_ns(&payload)
                {
                    let now_ns = unix_nanos_now();
                    let lat_ns = now_ns.saturating_sub(sent_ns);
                    let us_u128 = lat_ns / 1000;
                    let us = us_u128.min(u128::from(local.high())) as u64;
                    if us > 0 {
                        let _ = local.record(us);
                    }
                    recv.fetch_add(1, Ordering::Relaxed);
                    last_event_at = Instant::now();
                }
            }
        }
        Ok(())
    }
    .await;

    let mut h = hist.lock().await;
    merge(&mut h, &local);
    result
}

async fn run_writer(
    backend: &Backend,
    stream: &str,
    rate: u64,
    payload_size: usize,
    deadline: Instant,
) -> Result<(u64, Counts)> {
    let interval = Duration::from_micros(1_000_000 / rate.max(1));
    let mut next_at = Instant::now();
    let mut sent: u64 = 0;
    let mut ok: u64 = 0;
    let mut bp: u64 = 0;
    let mut err: u64 = 0;
    let mut seq: u64 = 0;
    while Instant::now() < deadline {
        let now = Instant::now();
        if now < next_at {
            tokio::time::sleep(next_at - now).await;
        }
        next_at += interval;
        let payload = build_payload(seq, payload_size);
        let resp = backend
            .append_request(0, stream, &payload, None, "text/plain")
            .send()
            .await;
        sent += 1;
        match resp {
            Ok(r) => {
                let s = r.status();
                if s.is_success() {
                    ok += 1;
                } else if s.as_u16() == 503 || s.as_u16() == 429 {
                    bp += 1;
                } else {
                    err += 1;
                }
            }
            Err(_) => err += 1,
        }
        seq += 1;
    }
    Ok((sent, Counts {
        ok,
        backpressure: bp,
        other_err: err,
    }))
}

fn build_payload(seq: u64, size: usize) -> Vec<u8> {
    // Layout: [16 hex chars seq] [32 hex chars send_ns] [' '] [filler]
    let now_ns = unix_nanos_now();
    let mut head = String::with_capacity(64);
    head.push_str(&format!("{seq:016x}"));
    head.push_str(&format!("{now_ns:032x}"));
    head.push(' ');
    let mut buf = Vec::with_capacity(size);
    let head_bytes = head.as_bytes();
    let take = head_bytes.len().min(size);
    buf.extend_from_slice(&head_bytes[..take]);
    if size > take {
        buf.resize(size, b'.');
    }
    buf
}

fn extract_send_ns(payload: &[u8]) -> Option<u128> {
    // Two layouts:
    //   - Ursula / DS raw text: 48 hex chars at byte 0..48 of the SSE data line.
    //   - S2 JSON envelope: the 48 hex chars appear inside "body":"<...>".
    // Scan for the first run of >=48 consecutive ASCII hex digits and parse
    // bytes 16..48 of that run as a u128 nanos timestamp.
    let mut run_start: Option<usize> = None;
    for (i, b) in payload.iter().enumerate() {
        if b.is_ascii_hexdigit() {
            let start = match run_start {
                Some(s) => s,
                None => {
                    run_start = Some(i);
                    i
                }
            };
            if i + 1 - start >= 48 {
                let s = std::str::from_utf8(&payload[start + 16..start + 48]).ok()?;
                return u128::from_str_radix(s, 16).ok();
            }
        } else {
            run_start = None;
        }
    }
    None
}

fn find_event_end(buf: &[u8]) -> Option<usize> {
    let mut prev = b'\0';
    for (i, b) in buf.iter().enumerate() {
        if prev == b'\n' && *b == b'\n' {
            return Some(i - 1);
        }
        prev = *b;
    }
    None
}

fn parse_sse_data(raw: &[u8]) -> Option<Vec<u8>> {
    let mut payload = Vec::new();
    for line in raw.split(|b| *b == b'\n') {
        if line.starts_with(b"data:") {
            let rest = &line[5..];
            let rest = if rest.starts_with(b" ") {
                &rest[1..]
            } else {
                rest
            };
            if !payload.is_empty() {
                payload.push(b'\n');
            }
            payload.extend_from_slice(rest);
        }
    }
    if payload.is_empty() {
        None
    } else {
        Some(payload)
    }
}

fn unix_nanos_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
