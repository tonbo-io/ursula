use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args;
use futures::stream::{FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::backend::{ApiStyle, Backend, Producer};
use crate::common::{
    Counts, LatencySummary, build_client, fill_payload, merge, new_histogram, record, summarize,
};

#[derive(Args, Debug, Clone)]
pub struct MultiStreamArgs {
    /// Target base URL(s). Comma-separated for round-robin across nodes.
    #[arg(long)]
    pub target: String,

    /// Backend API style.
    #[arg(long, value_enum, default_value_t = ApiStyle::Ursula)]
    pub api_style: ApiStyle,

    /// Bucket name (Ursula only - ignored by Durable / S2).
    #[arg(long, default_value = "bench-multistream")]
    pub bucket: String,

    /// Basin name (S2 only).
    #[arg(long, default_value = "benchmark")]
    pub basin: String,

    /// Number of concurrent streams; one writer task per stream.
    #[arg(long, default_value_t = 1000)]
    pub streams: usize,

    /// Wall-clock duration to drive load, in seconds.
    #[arg(long, default_value_t = 60)]
    pub duration_secs: u64,

    /// Payload size in bytes per append.
    #[arg(long, default_value_t = 256)]
    pub payload_bytes: usize,

    /// Target appends per second per stream. 0 = as fast as possible.
    #[arg(long, default_value_t = 0)]
    pub rate_per_stream: u64,

    /// Concurrent stream-creation calls during setup.
    #[arg(long, default_value_t = 256)]
    pub setup_concurrency: usize,

    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,
}

#[derive(Serialize)]
pub struct MultiStreamResult {
    pub scenario: &'static str,
    pub api_style: ApiStyle,
    pub target: String,
    pub bucket: String,
    pub basin: String,
    pub streams: usize,
    pub duration_secs: u64,
    pub payload_bytes: usize,
    pub rate_per_stream: u64,
    pub elapsed_secs: f64,
    pub counts: Counts,
    pub errors: Vec<ErrorCount>,
    pub aggregate_ops_per_sec: f64,
    pub per_stream_ops_per_sec_mean: f64,
    pub latency_ms: LatencySummary,
}

#[derive(Clone, Debug, Serialize)]
pub struct ErrorCount {
    pub error: String,
    pub count: u64,
}

pub async fn run(args: MultiStreamArgs) -> Result<MultiStreamResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(
        args.api_style,
        &args.target,
        &args.bucket,
        &args.basin,
        client,
    );

    tracing::info!(
        "creating namespace and streams: api={} streams={} targets={}",
        args.api_style.as_str(),
        args.streams,
        backend.bases.len()
    );
    backend.ensure_namespace().await?;
    create_streams(&backend, args.streams, args.setup_concurrency).await?;

    let payload = Arc::new(fill_payload(args.payload_bytes, 0xC0FFEE));
    let ok = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(Mutex::new(BTreeMap::<String, u64>::new()));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let start = Instant::now();

    let mut workers = Vec::with_capacity(args.streams);
    for idx in 0..args.streams {
        let backend = backend.clone();
        let stream = stream_name(idx);
        let payload = payload.clone();
        let ok = ok.clone();
        let bp = bp.clone();
        let err = err.clone();
        let errors = errors.clone();
        let hist = hist.clone();
        let rate = args.rate_per_stream;
        let producer_id = format!("bench-{idx}");
        workers.push(tokio::spawn(async move {
            run_writer(
                backend,
                idx,
                stream,
                payload,
                producer_id,
                rate,
                deadline,
                ok,
                bp,
                err,
                errors,
                hist,
            )
            .await
        }));
    }

    for w in workers {
        let _ = w.await;
    }

    let elapsed = start.elapsed();
    let counts = Counts {
        ok: ok.load(Ordering::Relaxed),
        backpressure: bp.load(Ordering::Relaxed),
        other_err: err.load(Ordering::Relaxed),
    };
    let errors = errors
        .lock()
        .await
        .iter()
        .map(|(error, count)| ErrorCount {
            error: error.clone(),
            count: *count,
        })
        .collect();
    let h = hist.lock().await;
    let latency = summarize(&h);
    let elapsed_secs = elapsed.as_secs_f64();
    let aggregate = counts.ok as f64 / elapsed_secs.max(1e-9);
    let per_stream_mean = aggregate / args.streams.max(1) as f64;

    Ok(MultiStreamResult {
        scenario: "multi-stream-write",
        api_style: args.api_style,
        target: args.target,
        bucket: args.bucket,
        basin: args.basin,
        streams: args.streams,
        duration_secs: args.duration_secs,
        payload_bytes: args.payload_bytes,
        rate_per_stream: args.rate_per_stream,
        elapsed_secs,
        counts,
        errors,
        aggregate_ops_per_sec: aggregate,
        per_stream_ops_per_sec_mean: per_stream_mean,
        latency_ms: latency,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_writer(
    backend: Backend,
    base_idx: usize,
    stream: String,
    payload: Arc<Vec<u8>>,
    producer_id: String,
    rate_per_stream: u64,
    deadline: Instant,
    ok: Arc<AtomicU64>,
    bp: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    errors: Arc<Mutex<BTreeMap<String, u64>>>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let epoch: u64 = 0;
    let mut seq: u64 = 0;
    let interval = if rate_per_stream > 0 {
        Some(Duration::from_micros(1_000_000 / rate_per_stream.max(1)))
    } else {
        None
    };
    let mut next_at = Instant::now();
    let mut local = new_histogram();
    let use_producer = matches!(backend.kind, ApiStyle::Ursula | ApiStyle::Durable);
    while Instant::now() < deadline {
        if let Some(iv) = interval {
            let now = Instant::now();
            if now < next_at {
                tokio::time::sleep(next_at - now).await;
            }
            next_at += iv;
        }
        let started = Instant::now();
        let producer = if use_producer {
            Some(Producer {
                id: &producer_id,
                epoch,
                seq,
            })
        } else {
            None
        };
        let resp = backend
            .append_request(
                base_idx,
                &stream,
                &payload,
                producer,
                "application/octet-stream",
            )
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                if status.is_success() {
                    ok.fetch_add(1, Ordering::Relaxed);
                    record(&mut local, started);
                    seq += 1;
                } else if status.as_u16() == 503 || status.as_u16() == 429 {
                    bp.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else {
                    err.fetch_add(1, Ordering::Relaxed);
                    record_error(&errors, format!("http_status_{}", status.as_u16())).await;
                }
            }
            Err(e) => {
                err.fetch_add(1, Ordering::Relaxed);
                record_error(&errors, reqwest_error_chain(&e)).await;
            }
        }
    }
    let mut h = hist.lock().await;
    merge(&mut h, &local);
}

async fn record_error(errors: &Mutex<BTreeMap<String, u64>>, error: String) {
    let mut errors = errors.lock().await;
    *errors.entry(error).or_default() += 1;
}

fn reqwest_error_chain(error: &reqwest::Error) -> String {
    let mut parts = Vec::new();
    let mut source = error.source();
    while let Some(err) = source {
        parts.push(err.to_string());
        source = err.source();
    }
    if parts.is_empty() {
        error.to_string()
    } else {
        parts.join(" | caused by: ")
    }
}

async fn create_streams(backend: &Backend, count: usize, concurrency: usize) -> Result<()> {
    let mut pending: FuturesUnordered<_> = FuturesUnordered::new();
    let mut next = 0usize;
    let max = concurrency.max(1);
    let push_one = |i: usize, pending: &mut FuturesUnordered<_>| {
        let backend = backend.clone();
        let stream = stream_name(i);
        pending.push(tokio::spawn(async move {
            backend
                .create_stream(&stream, "application/octet-stream")
                .await
        }));
    };
    while next < count && pending.len() < max {
        push_one(next, &mut pending);
        next += 1;
    }
    while let Some(joined) = pending.next().await {
        joined??;
        if next < count {
            push_one(next, &mut pending);
            next += 1;
        }
    }
    Ok(())
}

fn stream_name(idx: usize) -> String {
    format!("s{:08}", idx)
}
