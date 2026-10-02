//! W4: appends carrying `Producer-Id`/`Epoch`/`Seq`. Cold flush keeps hot bytes
//! bounded (8 MiB threshold) so producer state dominates. `--producers=P`
//! round-robins over P producer ids; `--epoch-every=E` bumps each producer's
//! epoch every E of its appends; `--retain-every=N` (W6) adds retention.

use std::time::Instant;

use anyhow::Result;
use anyhow::bail;
use clap::Args;
use serde_json::json;
use ursula_stream::ProducerRequest;
use ursula_stream::StreamResponse;
use ursula_stream::StreamStateMachine;

use crate::formula;
use crate::out::Baseline;
use crate::out::Outcome;
use crate::out::Sink;
use crate::out::measure_sm;
use crate::out::ratio;
use crate::out::round3;
use crate::payload;
use crate::smx;

#[derive(Debug, Clone, Args)]
pub struct W4Args {
    #[arg(long, default_value_t = 1_000_000)]
    pub appends: u64,
    #[arg(long, default_value_t = 1)]
    pub producers: u64,
    #[arg(long, default_value_t = 0)]
    pub epoch_every: u64,
    #[arg(long, default_value_t = 0)]
    pub retain_every: u64,
    #[arg(long, default_value_t = 2_000)]
    pub retain_keep: u64,
    #[arg(long, default_value_t = 200)]
    pub rec_bytes: usize,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "1000,10000,100000,1000000,3000000"
    )]
    pub checkpoints: Vec<u64>,
    /// Time duplicate lookups (newest and oldest sequence) at each checkpoint.
    #[arg(long)]
    pub dedup_timing: bool,
    #[arg(long)]
    pub zstd: bool,
    #[arg(long)]
    pub name: Option<String>,
}

pub fn default_name(args: &W4Args) -> String {
    args.name.clone().unwrap_or_else(|| {
        if args.retain_every > 0 {
            "w6_w4_retention".to_owned()
        } else if args.epoch_every > 0 {
            format!("w4_producer_epoch{}", args.epoch_every)
        } else {
            format!("w4_producer_p{}", args.producers)
        }
    })
}

pub fn run(args: &W4Args, sink: &mut Sink) -> Result<Outcome> {
    let name = default_name(args);
    sink.row(
        &json!({"workload": name, "appends": args.appends, "producers": args.producers,
        "epoch_every": args.epoch_every, "retain_every": args.retain_every,
        "retain_keep": args.retain_keep}),
    )?;
    let mut points: Vec<u64> = args
        .checkpoints
        .iter()
        .copied()
        .filter(|c| *c <= args.appends)
        .collect();
    if points.last() != Some(&args.appends) {
        points.push(args.appends);
    }
    let producers = usize::try_from(args.producers.max(1))?;
    let ids: Vec<String> = (0..producers).map(|p| format!("writer-{p:08}")).collect();
    let mut rng = payload::Rng::new(11);
    let base = Baseline::now();
    let mut m = StreamStateMachine::new();
    smx::create_bucket(&mut m, "bkt1")?;
    let id = smx::sid("bkt1", "h0001", "log");
    smx::create_stream(&mut m, &id, None, None, smx::T0)?;
    let mut seq = vec![0u64; producers];
    let mut epoch = vec![1u64; producers];
    let mut packs = smx::PackPaths::default();
    let mut stats = smx::FlushStats::default();
    let mut outcome = Outcome::default();
    let mut n = 0u64;
    let mut retentions = 0u64;
    let checkpoint_payload = br#"{"checkpoint":1}"#;
    let flush_bytes = 8 * smx::MIB;
    for cp in points {
        while n < cp {
            let p = usize::try_from(n % producers as u64)?;
            let (Some(s), Some(e), Some(producer_id)) =
                (seq.get_mut(p), epoch.get_mut(p), ids.get(p))
            else {
                bail!("producer index out of range");
            };
            if args.epoch_every > 0 && *s == args.epoch_every {
                *e += 1;
                *s = 0;
            }
            let producer = ProducerRequest {
                producer_id: producer_id.clone(),
                producer_epoch: *e,
                producer_seq: *s,
            };
            let now = smx::T0 + n * 10;
            let record = payload::json_record(&mut rng, n, args.rec_bytes);
            let response = smx::append(&mut m, &id, record, Some(producer), now);
            if let StreamResponse::Appended {
                deduplicated: true, ..
            } = response
            {
                bail!("unexpected deduplication at append {n}");
            }
            smx::ok(response, "producer append")?;
            *s += 1;
            n += 1;
            if m.total_hot_payload_bytes() >= flush_bytes as u64 {
                smx::flush_pass(&mut m, flush_bytes, flush_bytes, &mut packs, &mut stats)?;
            }
            if args.retain_every > 0 && n.is_multiple_of(args.retain_every) && n > args.retain_keep
            {
                smx::checkpoint_and_retain(
                    &mut m,
                    &id,
                    n - args.retain_keep,
                    checkpoint_payload,
                    now,
                )?;
                retentions += 1;
            }
        }
        let mut timing = Vec::new();
        if args.dedup_timing {
            let (Some(s0), Some(e0), Some(p0)) = (seq.first(), epoch.first(), ids.first()) else {
                bail!("no producers");
            };
            for (label, retry_seq) in [("newest", s0.saturating_sub(1)), ("oldest", 0u64)] {
                let reps = 50;
                let started = Instant::now();
                let mut deduplicated = false;
                for _ in 0..reps {
                    let response = smx::append(
                        &mut m,
                        &id,
                        b"{\"retry\":1}\n".to_vec(),
                        Some(ProducerRequest {
                            producer_id: p0.clone(),
                            producer_epoch: *e0,
                            producer_seq: retry_seq,
                        }),
                        smx::T0 + n * 10,
                    );
                    deduplicated = matches!(response, StreamResponse::Appended {
                        deduplicated: true,
                        ..
                    });
                }
                timing.push(json!({"retry_seq": label, "deduplicated": deduplicated,
                    "us_per_retry": round3(started.elapsed().as_secs_f64() * 1e6 / f64::from(reps))}));
            }
        }
        let measured = measure_sm(
            &m,
            &base,
            smx::append_counts(std::slice::from_ref(&id), &[n]),
            args.zstd,
        )?;
        formula::per_stream_checks(&mut outcome, &measured, 0);
        let heap = u64::try_from(measured.heap.bytes.max(0))?;
        sink.row(&json!({
            "workload": name,
            "appends": n,
            "retentions": retentions,
            "receipts_total": measured.snap.receipt_count,
            "snapshot_producer_bytes": measured.snap.producer_bytes,
            "snapshot_bytes_per_receipt": round3(ratio(measured.snap.producer_bytes, measured.snap.receipt_count)),
            "heap_bytes_per_append": round3(ratio(heap, n)),
            "dedup_lookup": timing,
            "m": measured.to_json(),
        }))?;
        if n == args.appends {
            outcome.metric_i64("heap_bytes", measured.heap.bytes);
            outcome.metric_u64("snapshot_bytes", measured.snap.total_bytes);
            outcome.metric_u64("snapshot_producer_bytes", measured.snap.producer_bytes);
            outcome.metric_u64("receipts", measured.gauges.receipts);
            outcome.metric_u64("receipt_items", measured.gauges.receipt_items);
            outcome.metric_u64("producers", measured.gauges.producers);
            outcome.metric_u64("producer_bytes", measured.gauges.producer_bytes);
        }
    }
    Ok(outcome)
}
