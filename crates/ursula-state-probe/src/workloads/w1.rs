//! W1: one JSON stream of ~200 B inline appends with cold flushes. With
//! `--retain-every=K` (W6): every K records publish a tiny checkpoint and
//! advance retention, keeping the last `--retain-keep` records.
#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]

use std::time::Instant;

use anyhow::Result;
use clap::Args;
use serde_json::json;
use ursula_stream::StreamStateMachine;

use crate::codec;
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
pub struct W1Args {
    /// Records to append.
    #[arg(long, default_value_t = 1_000_000)]
    pub records: u64,
    /// Bytes per record, LF included.
    #[arg(long, default_value_t = 200)]
    pub rec_bytes: usize,
    /// Records per append (a flattened JSON array body).
    #[arg(long, default_value_t = 1)]
    pub recs_per_append: u64,
    /// W6: checkpoint and retain every this many records (0 = never).
    #[arg(long, default_value_t = 0)]
    pub retain_every: u64,
    /// W6: records kept behind each retention point.
    #[arg(long, default_value_t = 2_000)]
    pub retain_keep: u64,
    /// Group flush threshold and maximum flush size, MiB.
    #[arg(long, default_value_t = 8)]
    pub flush_mib: usize,
    /// Comma-separated measurement points (records); the final count is
    /// always measured.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "1000,3000,10000,30000,100000,300000,1000000,3000000"
    )]
    pub checkpoints: Vec<u64>,
    /// Drain every hot byte before each measurement, so residual growth
    /// compares like with like (§7.2).
    #[arg(long)]
    pub forced_flush: bool,
    /// Also report zstd-3 snapshot sizes.
    #[arg(long)]
    pub zstd: bool,
    /// Also restore the final snapshot and report restored heap.
    #[arg(long)]
    pub restore: bool,
    /// Output name (JSONL file stem).
    #[arg(long)]
    pub name: Option<String>,
}

pub fn default_name(args: &W1Args) -> String {
    args.name.clone().unwrap_or_else(|| {
        if args.retain_every > 0 {
            "w6_w1_retention".to_owned()
        } else {
            "w1_inline".to_owned()
        }
    })
}

fn checkpoints(points: &[u64], total: u64) -> Vec<u64> {
    let mut out: Vec<u64> = points.iter().copied().filter(|c| *c <= total).collect();
    out.sort_unstable();
    out.dedup();
    if out.last() != Some(&total) {
        out.push(total);
    }
    out
}

pub fn run(args: &W1Args, sink: &mut Sink) -> Result<Outcome> {
    let name = default_name(args);
    sink.row(
        &json!({"workload": name, "records": args.records, "rec_bytes": args.rec_bytes,
        "recs_per_append": args.recs_per_append, "retain_every": args.retain_every,
        "retain_keep": args.retain_keep, "flush_threshold_mib": args.flush_mib,
        "forced_flush": args.forced_flush}),
    )?;

    let mut rng = payload::Rng::new(1);
    let per_append = args.recs_per_append.max(1);
    // Allocated before the heap baseline, so it does not count as state.
    let mut starts = smx::RecordStarts::new(if args.retain_every > 0 {
        usize::try_from(args.retain_keep / per_append)? + 2
    } else {
        0
    });
    let base = Baseline::now();
    let mut m = StreamStateMachine::new();
    smx::create_bucket(&mut m, "bkt1")?;
    let id = smx::sid("bkt1", "h0001", "log");
    smx::create_stream(&mut m, &id, None, None, smx::T0)?;

    let flush_bytes = args.flush_mib * smx::MIB;
    let mut packs = smx::PackPaths::default();
    let mut stats = smx::FlushStats::default();
    let mut outcome = Outcome::default();
    let mut n: u64 = 0;
    let mut appends: u64 = 0;
    let mut apply_ns: u128 = 0;
    let mut retentions = 0u64;
    let mut retained_from = 0u64;
    let mut next_retain = args.retain_every;
    let mut residuals = Vec::new();
    let checkpoint_payload = br#"{"checkpoint":1,"through":0}"#;
    let mut last = None;
    for cp in checkpoints(&args.checkpoints, args.records) {
        while n < cp {
            let now = smx::T0 + n * 10;
            let k = per_append.min(cp - n);
            let body = payload::json_records(&mut rng, n, usize::try_from(k)?, args.rec_bytes);
            starts.push(n, smx::tail(&m, &id));
            let started = Instant::now();
            let response = smx::append(&mut m, &id, body, None, now);
            apply_ns += started.elapsed().as_nanos();
            smx::ok(response, "append")?;
            appends += 1;
            n += k;
            if m.total_hot_payload_bytes() >= flush_bytes as u64 {
                smx::flush_pass(&mut m, flush_bytes, flush_bytes, &mut packs, &mut stats)?;
            }
            if args.retain_every > 0 && n >= next_retain {
                next_retain += args.retain_every;
                if n > args.retain_keep
                    && let Some((record, offset)) = starts.at_or_below(n - args.retain_keep)
                    && record > retained_from
                {
                    smx::checkpoint_and_retain(&mut m, &id, offset, checkpoint_payload, now)?;
                    retained_from = record;
                    retentions += 1;
                }
            }
        }
        if args.forced_flush {
            while !smx::flush_pass(&mut m, 1, flush_bytes, &mut packs, &mut stats)?.is_empty() {}
        }
        let tail = m.head(&id).map_or(0, |h| h.tail_offset);
        let hot = m.hot_payload_len(&id).unwrap_or(0);
        let measured = measure_sm(
            &m,
            &base,
            smx::append_counts(std::slice::from_ref(&id), &[appends]),
            args.zstd,
        )?;
        formula::per_stream_checks(&mut outcome, &measured, 0);
        if args.retain_every > 0 && retentions > 0 {
            formula::capacity_check(&mut outcome, &measured);
        }
        let residual = formula::residual(&measured);
        residuals.push((n, residual));
        let retained = n - retained_from;
        sink.row(&json!({
            "workload": name,
            "records": n,
            "appends": appends,
            "logical_bytes": tail,
            "retained_records": retained,
            "retained_from_record": retained_from,
            "hot_bytes": hot,
            "cold_refs_in_state": m.cold_chunks(&id).len(),
            "flush": stats,
            "retentions": retentions,
            "residual_bytes": residual,
            "apply_avg_us": round3(apply_ns as f64 / appends.max(1) as f64 / 1e3),
            "per_record": {
                "heap_bytes": round3(ratio(u64::try_from(measured.heap.bytes.max(0))?, n)),
                "snapshot_bytes": round3(ratio(measured.snap.total_bytes, n)),
            },
            "m": measured.to_json(),
        }))?;
        last = Some((measured, retained));
    }

    // Residual growth between N and 4N (§7.2), when both were measured.
    if let Some(&(n_last, r_last)) = residuals.last()
        && let Some(&(_, r_quarter)) = residuals.iter().find(|(k, _)| k * 4 == n_last)
    {
        outcome.check(
            "residual_growth_n_to_4n",
            "residual state grows <= 1 KiB between N and 4N records",
            (r_last - r_quarter) as f64,
            1024.0,
        );
    }
    if let Some((measured, retained)) = &last {
        outcome.metric_u64("records", n);
        outcome.metric_u64("retained_records", *retained);
        outcome.metric_i64("heap_bytes", measured.heap.bytes);
        outcome.metric_i64("heap_tight_bytes", measured.tight.bytes);
        outcome.metric_u64("snapshot_bytes", measured.snap.total_bytes);
        outcome.metric_u64("flush_passes", stats.passes);
        outcome.metric_u64("hot_chunks", measured.gauges.hot_chunks);
        if let Some(&(_, residual)) = residuals.last() {
            outcome.metric_i64("residual_bytes", residual);
        }
    }

    if args.restore {
        let snapshot = codec::group_snapshot(m.snapshot(), Vec::new(), 0);
        let bytes = codec::encode_bytes(snapshot)?;
        drop(m);
        let before = crate::alloc::heap();
        let started = Instant::now();
        let decoded = codec::decode(&bytes)?;
        let restored = StreamStateMachine::restore(decoded.stream_snapshot)
            .map_err(|err| anyhow::anyhow!("restore: {err:?}"))?;
        let restore_ms = started.elapsed().as_secs_f64() * 1e3;
        let after = crate::alloc::heap();
        sink.row(&json!({
            "workload": name,
            "restore_from_snapshot": {
                "snapshot_bytes": bytes.len(),
                "restored_heap_bytes": (after - before).bytes,
                "decode_and_restore_ms": round3(restore_ms),
                "restored_tail": smx::tail(&restored, &id),
            }
        }))?;
    }
    Ok(outcome)
}
