//! Serialized single-writer `Stream-Record-Match` appends (keyed-streams M0c).
//!
//! Each writer owns one `application/json` stream and keeps exactly one append
//! in flight. Every append carries `Stream-Record-Match: <n>`, where `n` is the
//! `Stream-Record-Next` returned by the previous append (0 for a fresh
//! stream). This is the commit path a Pi Durable owner uses: one Session line
//! per harness, one commit in flight, each commit conditional on the previous
//! one. Bodies are single JSON objects shaped like `keyed-batch-v1` records so
//! each append is exactly one record.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use clap::Args;
use clap::ValueEnum;
use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::backend::ApiStyle;
use crate::backend::Backend;
use crate::common::LatencySummary;
use crate::common::build_client;
use crate::common::merge;
use crate::common::new_histogram;
use crate::common::record;
use crate::common::summarize;

const HEADER_RECORD_MATCH: &str = "stream-record-match";
const HEADER_RECORD_NEXT: &str = "stream-record-next";

/// Payload size distribution for each commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[clap(rename_all = "lower")]
#[serde(rename_all = "lowercase")]
pub enum PayloadMix {
    /// Every commit is `--payload-bytes` long.
    Fixed,
    /// Approximation of the Pi commit mix measured by the instrumented probe:
    /// 70% 127 B partials, 15% 200-700 B task/entry commits, 13% 1-4 KiB
    /// tool-output deltas and turn settles, 2% 13-50 KiB tool settles.
    Pi,
}

#[derive(Args, Debug, Clone)]
pub struct RecordMatchArgs {
    /// Target base URL(s). Comma-separated for round-robin across nodes.
    #[arg(long)]
    pub target: String,

    /// Bucket name.
    #[arg(long, default_value = "bench-record-match")]
    pub bucket: String,

    /// Stream name prefix; writer `i` uses `<prefix>-<i>`. A unique prefix per
    /// run keeps record counters fresh.
    #[arg(long, default_value = "rm")]
    pub stream_prefix: String,

    /// Number of concurrent writers, each on its own stream.
    #[arg(long, default_value_t = 1)]
    pub writers: usize,

    /// Measured duration in seconds (after warm-up).
    #[arg(long, default_value_t = 30)]
    pub duration_secs: u64,

    /// Warm-up seconds whose samples are discarded.
    #[arg(long, default_value_t = 3)]
    pub warmup_secs: u64,

    /// Stream and append content type.
    #[arg(long, default_value = "application/json")]
    pub content_type: String,

    /// Payload size distribution.
    #[arg(long, value_enum, default_value_t = PayloadMix::Pi)]
    pub payload_mix: PayloadMix,

    /// Payload size for `--payload-mix fixed`.
    #[arg(long, default_value_t = 127)]
    pub payload_bytes: usize,

    /// Idle time between a commit settling and the next one, in milliseconds
    /// (0 = closed loop, the next commit starts as soon as the previous one
    /// settles).
    #[arg(long, default_value_t = 0)]
    pub think_ms: u64,

    /// PRNG seed for the payload mix.
    #[arg(long, default_value_t = 0x5EED)]
    pub seed: u64,

    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,

    /// Optional path for raw measured samples, one `<latency_us> <bytes>` line
    /// per commit (input for the §9.2 line-model simulation).
    #[arg(long)]
    pub samples_out: Option<std::path::PathBuf>,
}

#[derive(Serialize)]
pub struct RecordMatchResult {
    pub scenario: &'static str,
    pub target: String,
    pub writers: usize,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub content_type: String,
    pub payload_mix: PayloadMix,
    pub think_ms: u64,
    pub elapsed_secs: f64,
    pub commits: u64,
    pub commit_bytes: u64,
    pub mean_commit_bytes: f64,
    pub commits_per_sec: f64,
    pub per_writer_commits_per_sec: f64,
    pub precondition_failed: u64,
    pub backpressure: u64,
    pub errors: BTreeMap<String, u64>,
    pub latency_ms: LatencySummary,
    /// Latency split by payload class (`<=512B`, `<=4KiB`, `>4KiB`).
    pub latency_by_size_ms: BTreeMap<String, LatencySummary>,
}

#[derive(Default)]
struct WriterOutcome {
    samples: Vec<(u64, u64)>,
    commits: u64,
    bytes: u64,
    precondition_failed: u64,
    backpressure: u64,
    errors: BTreeMap<String, u64>,
}

pub async fn run(args: RecordMatchArgs) -> Result<RecordMatchResult> {
    let client = build_client(args.request_timeout_secs)?;
    let backend = Backend::new(ApiStyle::Ursula, &args.target, &args.bucket, client);
    backend.ensure_namespace().await?;
    for idx in 0..args.writers {
        create_with_retry(
            &backend,
            &stream_name(&args.stream_prefix, idx),
            &args.content_type,
        )
        .await?;
    }

    let hists = Arc::new(Mutex::new(SizeHistograms::new()));
    let start = Instant::now();
    let measure_from = start + Duration::from_secs(args.warmup_secs);
    let deadline = measure_from + Duration::from_secs(args.duration_secs);

    let mut workers = Vec::with_capacity(args.writers);
    for idx in 0..args.writers {
        let backend = backend.clone();
        let args = args.clone();
        let hists = hists.clone();
        workers.push(tokio::spawn(async move {
            run_writer(backend, args, idx, measure_from, deadline, hists).await
        }));
    }
    let mut total = WriterOutcome::default();
    for worker in workers {
        let outcome = worker.await?;
        total.samples.extend_from_slice(&outcome.samples);
        total.commits += outcome.commits;
        total.bytes += outcome.bytes;
        total.precondition_failed += outcome.precondition_failed;
        total.backpressure += outcome.backpressure;
        for (error, count) in outcome.errors {
            *total.errors.entry(error).or_default() += count;
        }
    }
    let measured = Instant::now()
        .saturating_duration_since(measure_from)
        .as_secs_f64()
        .max(1e-9);
    if let Some(path) = &args.samples_out {
        use std::fmt::Write as _;
        let mut text = String::with_capacity(total.samples.len() * 16);
        for (us, bytes) in &total.samples {
            let _ = writeln!(text, "{us} {bytes}");
        }
        std::fs::write(path, text)?;
    }
    let hists = hists.lock().await;
    let commits_per_sec = total.commits as f64 / measured;
    Ok(RecordMatchResult {
        scenario: "record-match-serialized",
        target: args.target,
        writers: args.writers,
        duration_secs: args.duration_secs,
        warmup_secs: args.warmup_secs,
        content_type: args.content_type,
        payload_mix: args.payload_mix,
        think_ms: args.think_ms,
        elapsed_secs: measured,
        commits: total.commits,
        commit_bytes: total.bytes,
        mean_commit_bytes: total.bytes as f64 / (total.commits.max(1) as f64),
        commits_per_sec,
        per_writer_commits_per_sec: commits_per_sec / args.writers.max(1) as f64,
        precondition_failed: total.precondition_failed,
        backpressure: total.backpressure,
        errors: total.errors,
        latency_ms: summarize(&hists.all),
        latency_by_size_ms: hists
            .by_size
            .iter()
            .map(|(label, hist)| ((*label).to_owned(), summarize(hist)))
            .collect(),
    })
}

struct SizeHistograms {
    all: Histogram<u64>,
    by_size: BTreeMap<&'static str, Histogram<u64>>,
}

impl SizeHistograms {
    fn new() -> Self {
        Self {
            all: new_histogram(),
            by_size: BTreeMap::new(),
        }
    }

    fn merge_from(&mut self, other: &SizeHistograms) {
        merge(&mut self.all, &other.all);
        for (label, hist) in &other.by_size {
            merge(
                self.by_size.entry(label).or_insert_with(new_histogram),
                hist,
            );
        }
    }

    fn record(&mut self, size: usize, started: Instant) {
        record(&mut self.all, started);
        record(
            self.by_size
                .entry(size_class(size))
                .or_insert_with(new_histogram),
            started,
        );
    }
}

fn size_class(size: usize) -> &'static str {
    if size <= 512 {
        "a_le_512B"
    } else if size <= 4096 {
        "b_le_4KiB"
    } else {
        "c_gt_4KiB"
    }
}

async fn run_writer(
    backend: Backend,
    args: RecordMatchArgs,
    idx: usize,
    measure_from: Instant,
    deadline: Instant,
    hists: Arc<Mutex<SizeHistograms>>,
) -> WriterOutcome {
    let stream = stream_name(&args.stream_prefix, idx);
    let mut rng = SplitMix64::new(args.seed ^ (idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut local = SizeHistograms::new();
    let mut outcome = WriterOutcome::default();
    let mut next_record: u64 = 0;
    let mut op: u64 = 0;
    while Instant::now() < deadline {
        let size = match args.payload_mix {
            PayloadMix::Fixed => args.payload_bytes,
            PayloadMix::Pi => pi_mix_size(&mut rng),
        };
        let body = keyed_batch_body(op, size);
        op += 1;
        let started = Instant::now();
        let resp = backend
            .append_request(idx, &stream, &body, None, &args.content_type)
            .header(HEADER_RECORD_MATCH, next_record.to_string())
            .send()
            .await;
        let measuring = started >= measure_from;
        match resp {
            Ok(resp) => {
                let status = resp.status();
                let next = resp
                    .headers()
                    .get(HEADER_RECORD_NEXT)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok());
                if status.is_success() {
                    if measuring {
                        let us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                        outcome.samples.push((us, body.len() as u64));
                        local.record(body.len(), started);
                        outcome.commits += 1;
                        outcome.bytes += body.len() as u64;
                    }
                    match next {
                        Some(next) => next_record = next,
                        None => {
                            *outcome
                                .errors
                                .entry("missing_stream_record_next".to_owned())
                                .or_default() += 1;
                            next_record += 1;
                        }
                    }
                } else if status.as_u16() == 412 {
                    // Another writer moved the tail (or a retried append landed):
                    // resynchronise from the reported record tail.
                    outcome.precondition_failed += 1;
                    if let Some(next) = next {
                        next_record = next;
                    }
                } else if status.as_u16() == 503 || status.as_u16() == 429 {
                    outcome.backpressure += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                } else {
                    let body = resp.text().await.unwrap_or_default();
                    let mut key = format!("http_{}: {}", status.as_u16(), body);
                    key.truncate(160);
                    *outcome.errors.entry(key).or_default() += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            Err(error) => {
                let mut key = format!("transport: {error}");
                key.truncate(160);
                *outcome.errors.entry(key).or_default() += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        if args.think_ms > 0 {
            tokio::time::sleep(Duration::from_millis(args.think_ms)).await;
        }
    }
    hists.lock().await.merge_from(&local);
    outcome
}

/// Creates a stream, retrying while the cluster answers with backpressure
/// (503/429 under background load) for up to ~10 s.
async fn create_with_retry(backend: &Backend, stream: &str, content_type: &str) -> Result<()> {
    let mut attempt = 0u32;
    loop {
        match backend.create_stream(stream, content_type).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt < 500 => {
                attempt += 1;
                tracing::debug!(%error, attempt, "retrying stream create");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn stream_name(prefix: &str, idx: usize) -> String {
    format!("{prefix}-{idx:04}")
}

/// Samples one commit size from the Pi approximation (see [`PayloadMix::Pi`]).
pub fn pi_mix_size(rng: &mut SplitMix64) -> usize {
    let class = rng.next_u64() % 100;
    let uniform = |rng: &mut SplitMix64, lo: u64, hi: u64| -> usize {
        (lo + rng.next_u64() % (hi - lo + 1)) as usize
    };
    if class < 70 {
        127
    } else if class < 85 {
        uniform(rng, 200, 700)
    } else if class < 98 {
        uniform(rng, 1024, 4096)
    } else {
        uniform(rng, 13 * 1024, 50 * 1024)
    }
}

/// Builds a single JSON object of exactly `size` bytes (minimum 40) shaped like
/// a `keyed-batch-v1` record with one put op: `{"o":N,"ops":[["p","k",{"t":"..."}]]}`.
pub fn keyed_batch_body(op: u64, size: usize) -> Bytes {
    let prefix = format!("{{\"o\":{op},\"ops\":[[\"p\",\"AWJlbmNoAA\",{{\"t\":\"");
    let suffix = "\"}]]}";
    let fixed = prefix.len() + suffix.len();
    let pad = size.saturating_sub(fixed);
    let mut out = String::with_capacity(fixed + pad);
    out.push_str(&prefix);
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789 ";
    for i in 0..pad {
        let ch = ALPHABET
            .get((i * 7 + op as usize) % ALPHABET.len())
            .copied();
        out.push(char::from(ch.unwrap_or(b'x')));
    }
    out.push_str(suffix);
    Bytes::from(out)
}

/// Small deterministic PRNG so the payload mix is reproducible per seed.
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::SplitMix64;
    use super::keyed_batch_body;
    use super::pi_mix_size;

    #[test]
    fn keyed_batch_body_is_one_json_object_of_requested_size() {
        for size in [127usize, 300, 4096, 50 * 1024] {
            let body = keyed_batch_body(42, size);
            assert_eq!(body.len(), size);
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(value.is_object());
            assert_eq!(value["o"], 42);
        }
    }

    #[test]
    fn pi_mix_is_dominated_by_small_partials() {
        let mut rng = SplitMix64::new(7);
        let sizes: Vec<usize> = (0..100_000).map(|_| pi_mix_size(&mut rng)).collect();
        let small = sizes.iter().filter(|s| **s == 127).count() as f64 / sizes.len() as f64;
        let large =
            sizes.iter().filter(|s| **s > 13 * 1024 - 1).count() as f64 / sizes.len() as f64;
        assert!((0.68..0.72).contains(&small), "small share {small}");
        assert!((0.015..0.025).contains(&large), "large share {large}");
        assert!(sizes.iter().all(|s| *s <= 50 * 1024));
    }
}
