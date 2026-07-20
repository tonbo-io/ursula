use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
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
use crate::common::fill_payload;
use crate::common::new_histogram;
use crate::common::summarize;

#[derive(Args, Debug, Clone)]
pub struct BootstrapArgs {
    #[arg(long)]
    pub target: String,

    #[arg(long, value_enum, default_value_t = ApiStyle::Ursula)]
    pub api_style: ApiStyle,

    #[arg(long, default_value = "bench-bootstrap")]
    pub bucket: String,

    #[arg(long, default_value = "doc")]
    pub stream: String,

    #[arg(long, default_value_t = 500)]
    pub clients: usize,

    #[arg(long, default_value_t = 2000)]
    pub pre_events: usize,

    #[arg(long, default_value_t = 1024)]
    pub event_bytes: usize,

    #[arg(long, default_value_t = 65536)]
    pub snapshot_bytes: usize,

    #[arg(long, default_value_t = 32)]
    pub setup_concurrency: usize,

    #[arg(long, default_value_t = 120)]
    pub request_timeout_secs: u64,

    #[arg(long, default_value_t = false)]
    pub per_client_stream: bool,
}

#[derive(Serialize)]
pub struct BootstrapResult {
    pub scenario: &'static str,
    pub api_style: ApiStyle,
    pub target: String,
    pub bucket: String,
    pub stream: String,
    pub per_client_stream: bool,
    pub clients: usize,
    pub pre_events: usize,
    pub event_bytes: usize,
    pub snapshot_bytes: usize,
    pub setup_elapsed_secs: f64,
    pub stampede_elapsed_secs: f64,
    pub counts: Counts,
    pub bytes_received_total: u64,
    pub latency_ms: LatencySummary,
}

pub async fn run(args: BootstrapArgs) -> Result<BootstrapResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(args.api_style, &args.target, &args.bucket, client);

    backend.ensure_namespace().await?;

    let stream_names: Vec<String> = if args.per_client_stream {
        (0..args.clients)
            .map(|i| format!("{}-{i:06}", args.stream))
            .collect()
    } else {
        vec![args.stream.clone()]
    };

    let setup_start = Instant::now();
    let payload = Arc::new(fill_payload(args.event_bytes, 0xBEEF));
    let event_count = args.pre_events;
    let snapshot_offset_bytes = (event_count as u64 / 2) * args.event_bytes as u64;
    let pending = Arc::new(tokio::sync::Semaphore::new(args.setup_concurrency.max(1)));

    for stream in &stream_names {
        let _ = backend.delete_stream(stream).await;
        backend
            .create_stream(stream, "application/octet-stream")
            .await
            .with_context(|| format!("create_stream {stream}"))?;
    }

    let mut joins = Vec::new();
    for stream in &stream_names {
        for _ in 0..event_count {
            let permit = pending.clone().acquire_owned().await.unwrap();
            let backend = backend.clone();
            let stream = stream.clone();
            let payload = payload.clone();
            joins.push(tokio::spawn(async move {
                let _permit = permit;
                let _ = backend
                    .append_request(0, &stream, &payload, None, "application/octet-stream")
                    .send()
                    .await;
            }));
        }
    }
    for j in joins {
        let _ = j.await;
    }

    if args.snapshot_bytes > 0 {
        let snap = Bytes::from(fill_payload(args.snapshot_bytes, 0x5A5A));
        for stream in &stream_names {
            backend
                .publish_snapshot(stream, snapshot_offset_bytes, snap.clone())
                .await
                .with_context(|| format!("publish snapshot for {stream}"))?;
        }
    }
    let setup_elapsed = setup_start.elapsed();

    let barrier = Arc::new(Barrier::new(args.clients));
    let ok = Arc::new(AtomicU64::new(0));
    let bp = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(new_histogram()));

    let mut handles = Vec::with_capacity(args.clients);
    for idx in 0..args.clients {
        let backend = backend.clone();
        let stream = if args.per_client_stream {
            stream_names[idx].clone()
        } else {
            stream_names[0].clone()
        };
        let barrier = barrier.clone();
        let ok = ok.clone();
        let bp = bp.clone();
        let err = err.clone();
        let bytes_total = bytes_total.clone();
        let hist = hist.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            run_client(&backend, idx, &stream, ok, bp, err, bytes_total, hist).await;
        }));
    }

    let stampede_start = Instant::now();
    for h in handles {
        let _ = h.await;
    }
    let stampede_elapsed = stampede_start.elapsed();

    let h = hist.lock().await;
    let latency = summarize(&h);

    Ok(BootstrapResult {
        scenario: "bootstrap-stampede",
        api_style: args.api_style,
        target: args.target,
        bucket: args.bucket,
        stream: args.stream,
        per_client_stream: args.per_client_stream,
        clients: args.clients,
        pre_events: args.pre_events,
        event_bytes: args.event_bytes,
        snapshot_bytes: args.snapshot_bytes,
        setup_elapsed_secs: setup_elapsed.as_secs_f64(),
        stampede_elapsed_secs: stampede_elapsed.as_secs_f64(),
        counts: Counts {
            ok: ok.load(Ordering::Relaxed),
            backpressure: bp.load(Ordering::Relaxed),
            other_err: err.load(Ordering::Relaxed),
        },
        bytes_received_total: bytes_total.load(Ordering::Relaxed),
        latency_ms: latency,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_client(
    backend: &Backend,
    base_idx: usize,
    stream: &str,
    ok: Arc<AtomicU64>,
    bp: Arc<AtomicU64>,
    err: Arc<AtomicU64>,
    bytes_total: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
) {
    let started = Instant::now();
    let req = backend.replay_request_for(base_idx, stream);
    let resp = match req.send().await {
        Ok(r) => r,
        Err(_) => {
            err.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let status = resp.status();
    if !status.is_success() {
        if status.as_u16() == 503 {
            bp.fetch_add(1, Ordering::Relaxed);
        } else {
            err.fetch_add(1, Ordering::Relaxed);
        }
        return;
    }
    let mut bytes = 0u64;
    let mut s = resp.bytes_stream();
    while let Some(chunk) = s.next().await {
        match chunk {
            Ok(c) => bytes += c.len() as u64,
            Err(_) => {
                err.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
    }
    let elapsed = started.elapsed();
    let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
    let mut h = hist.lock().await;
    let us = us.min(h.high()).max(h.low());
    let _ = h.record(us);
    drop(h);
    bytes_total.fetch_add(bytes, Ordering::Relaxed);
    ok.fetch_add(1, Ordering::Relaxed);
}
