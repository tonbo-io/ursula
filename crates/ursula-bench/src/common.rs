use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use reqwest::Client;
use serde::Serialize;

pub fn build_client(timeout_secs: u64) -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .pool_max_idle_per_host(512)
        .tcp_nodelay(true)
        .build()
        .context("build reqwest client")
}

pub fn new_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("hist bounds")
}

pub fn record(hist: &mut Histogram<u64>, started_at: Instant) {
    let us = started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
    let us = us.min(hist.high());
    let _ = hist.record(us.max(hist.low()));
}

#[derive(Default, Clone, Debug, Serialize)]
pub struct LatencySummary {
    pub count: u64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
}

pub fn summarize(hist: &Histogram<u64>) -> LatencySummary {
    if hist.is_empty() {
        return LatencySummary::default();
    }
    let to_ms = |v: u64| (v as f64) / 1000.0;
    LatencySummary {
        count: hist.len(),
        mean_ms: hist.mean() / 1000.0,
        p50_ms: to_ms(hist.value_at_quantile(0.5)),
        p90_ms: to_ms(hist.value_at_quantile(0.9)),
        p99_ms: to_ms(hist.value_at_quantile(0.99)),
        p999_ms: to_ms(hist.value_at_quantile(0.999)),
        max_ms: to_ms(hist.max()),
    }
}

pub fn merge(target: &mut Histogram<u64>, other: &Histogram<u64>) {
    target.add(other).expect("histogram bounds match");
}

pub fn fill_payload(size: usize, seed: u64) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    let mut state = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
    for chunk in buf.chunks_mut(8) {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    buf
}

#[derive(Clone, Debug, Serialize)]
pub struct Counts {
    pub ok: u64,
    pub backpressure: u64,
    pub other_err: u64,
}
