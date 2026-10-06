//! W5: stream and bucket churn and TTL.
//!
//! - `delete`: create N streams per cycle (appends + drain flush), delete all,
//!   drain cold GC; repeated cycles.
//! - `ttl-expire`: N streams with sliding TTL, then let them expire (sweeps run
//!   on later writes).
//! - `ttl-heap`: one long-lived stream, A appends, TTL none / sliding /
//!   absolute: TTL heap growth per append.
//! - `purge`: B buckets with one stream each, `PurgeBucket` each.

use anyhow::Result;
use anyhow::bail;
use clap::Args;
use clap::ValueEnum;
use serde_json::json;
use ursula_stream::StreamCommand;
use ursula_stream::StreamStateMachine;

use crate::alloc;
use crate::formula;
use crate::out::Baseline;
use crate::out::Outcome;
use crate::out::Sink;
use crate::out::measure_sm;
use crate::out::ratio;
use crate::out::round3;
use crate::payload;
use crate::smx;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum W5Mode {
    Delete,
    TtlExpire,
    TtlHeap,
    Purge,
}

impl W5Mode {
    fn label(self) -> &'static str {
        match self {
            W5Mode::Delete => "delete",
            W5Mode::TtlExpire => "ttl-expire",
            W5Mode::TtlHeap => "ttl-heap",
            W5Mode::Purge => "purge",
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct W5Args {
    #[arg(long, value_enum, default_value = "delete")]
    pub mode: W5Mode,
    /// Streams per cycle (`delete`) or in total (`ttl-expire`).
    #[arg(long, default_value_t = 10_000)]
    pub streams: usize,
    #[arg(long, default_value_t = 10)]
    pub cycles: usize,
    #[arg(long, default_value_t = 5)]
    pub appends_per_stream: usize,
    /// `ttl-heap`: appends to the long-lived stream.
    #[arg(long, default_value_t = 1_000_000)]
    pub appends: u64,
    /// `ttl-expire` default 60 s; `ttl-heap` uses `--ttl-heap-seconds`.
    #[arg(long, default_value_t = 60)]
    pub ttl_seconds: u64,
    #[arg(long, default_value_t = 7 * 86_400)]
    pub ttl_heap_seconds: u64,
    /// `ttl-heap` (W6): retention every this many appends.
    #[arg(long, default_value_t = 0)]
    pub retain_every: u64,
    #[arg(long, default_value_t = 100_000)]
    pub buckets: usize,
    #[arg(long)]
    pub name: Option<String>,
}

pub fn default_name(args: &W5Args) -> String {
    args.name
        .clone()
        .unwrap_or_else(|| format!("w5_churn_{}", args.mode.label()))
}

pub fn run(args: &W5Args, sink: &mut Sink) -> Result<Outcome> {
    match args.mode {
        W5Mode::Delete => delete(args, sink),
        W5Mode::TtlExpire => ttl_expire(args, sink),
        W5Mode::TtlHeap => ttl_heap(args, sink),
        W5Mode::Purge => purge(args, sink),
    }
}

fn delete(args: &W5Args, sink: &mut Sink) -> Result<Outcome> {
    sink.row(
        &json!({"mode": "delete", "streams_per_cycle": args.streams, "cycles": args.cycles,
        "appends_per_stream": args.appends_per_stream}),
    )?;
    let mut rng = payload::Rng::new(21);
    let base = Baseline::now();
    let mut m = StreamStateMachine::new();
    smx::create_bucket(&mut m, "bkt1")?;
    let keep = smx::sid("bkt1", "keep", "log");
    smx::create_stream(&mut m, &keep, None, None, smx::T0)?;
    let mut packs = smx::PackPaths::default();
    let mut stats = smx::FlushStats::default();
    let mut outcome = Outcome::default();
    let mut created_total = 0usize;
    for cycle in 0..args.cycles {
        let ids: Vec<_> = (0..args.streams)
            .map(|i| {
                smx::sid(
                    "bkt1",
                    &format!("c{cycle:03}-{i:06}"),
                    &format!("log{i:06}"),
                )
            })
            .collect();
        let now = smx::T0.saturating_add((cycle as u64).saturating_mul(60_000));
        for id in &ids {
            smx::create_stream(&mut m, id, None, None, now)?;
            for k in 0..args.appends_per_stream {
                let record = payload::json_record(&mut rng, k as u64, 200);
                smx::ok(smx::append(&mut m, id, record, None, now), "append")?;
            }
        }
        while !smx::flush_pass(&mut m, 1, 8 * smx::MIB, &mut packs, &mut stats)?.is_empty() {}
        let live = measure_sm(&m, &base, Vec::new(), false)?;
        for id in &ids {
            smx::ok(
                m.apply(StreamCommand::DeleteStream {
                    stream_id: id.clone(),
                }),
                "delete",
            )?;
        }
        created_total = created_total.saturating_add(args.streams);
        let pending = m.pending_cold_gc_len();
        smx::ack_all_cold_gc(&mut m)?;
        let after_gc = measure_sm(&m, &base, Vec::new(), false)?;
        sink.row(&json!({
            "cycle": cycle, "streams_created_total": created_total,
            "flush": stats,
            "pending_cold_gc_after_delete": pending,
            "live": {"heap": live.heap.bytes, "snapshot": live.snap.total_bytes},
            "after_gc_ack": after_gc.to_json(),
        }))?;
        if args.cycles.checked_sub(1) == Some(cycle) {
            outcome.metric_i64("heap_after_churn_bytes", after_gc.heap.bytes);
            outcome.metric_i64("heap_slack_after_churn_bytes", after_gc.slack_bytes());
            outcome.metric_u64("snapshot_after_churn_bytes", after_gc.snap.total_bytes);
            outcome.metric_u64("live_packs_after_churn", after_gc.gauges.live_packs);
        }
    }
    Ok(outcome)
}

fn ttl_expire(args: &W5Args, sink: &mut Sink) -> Result<Outcome> {
    let streams = args.streams;
    let ttl = args.ttl_seconds;
    sink.row(&json!({"mode": "ttl-expire", "streams": streams,
        "appends_per_stream": args.appends_per_stream, "ttl_seconds": ttl}))?;
    let mut rng = payload::Rng::new(22);
    let base = Baseline::now();
    let mut m = StreamStateMachine::new();
    smx::create_bucket(&mut m, "bkt1")?;
    let keep = smx::sid("bkt1", "keep", "log");
    smx::create_stream(&mut m, &keep, None, None, smx::T0)?;
    let mut packs = smx::PackPaths::default();
    let mut stats = smx::FlushStats::default();
    let mut outcome = Outcome::default();
    let ids: Vec<_> = (0..streams)
        .map(|i| smx::sid("bkt1", &format!("t{i:07}"), &format!("log{i:07}")))
        .collect();
    for (i, id) in ids.iter().enumerate() {
        let now = smx::T0.saturating_add(i as u64);
        smx::create_stream(&mut m, id, Some(ttl), None, now)?;
        for k in 0..args.appends_per_stream {
            let record = payload::json_record(&mut rng, k as u64, 200);
            smx::ok(
                smx::append(&mut m, id, record, None, now.saturating_add(k as u64)),
                "append",
            )?;
        }
    }
    while !smx::flush_pass(&mut m, 1, 8 * smx::MIB, &mut packs, &mut stats)?.is_empty() {}
    let live = measure_sm(&m, &base, Vec::new(), false)?;
    formula::per_stream_checks(&mut outcome, &live, 0);
    outcome.metric_u64("ttl_heap_entries_live", live.gauges.ttl_heap_entries);
    sink.row(&json!({"phase": "all TTL streams live", "m": live.to_json()}))?;
    drop(live);
    let later = smx::T0
        .saturating_add(streams as u64)
        .saturating_add(args.appends_per_stream as u64)
        .saturating_add(ttl.saturating_mul(1000))
        .saturating_add(1);
    let mut writes = 0u64;
    let limit = (streams as u64 / 200).saturating_add(100).saturating_mul(2);
    while writes <= limit && ids.iter().any(|id| m.head(id).is_some()) {
        let record = payload::json_record(&mut rng, writes, 200);
        smx::ok(
            smx::append(&mut m, &keep, record, None, later.saturating_add(writes)),
            "keepalive",
        )?;
        writes = writes.saturating_add(1);
    }
    let left = ids.iter().filter(|id| m.head(id).is_some()).count();
    let pending = m.pending_cold_gc_len();
    smx::ack_all_cold_gc(&mut m)?;
    let after = measure_sm(&m, &base, Vec::new(), false)?;
    formula::per_stream_checks(&mut outcome, &after, 0);
    sink.row(
        &json!({"phase": "after expiry sweeps and GC ack", "keepalive_writes_needed": writes,
        "streams_left": left, "pending_cold_gc_before_ack": pending, "m": after.to_json()}),
    )?;
    outcome.metric_u64("keepalive_writes_needed", writes);
    outcome.metric_u64(
        "ttl_heap_entries_after_expiry",
        after.gauges.ttl_heap_entries,
    );
    outcome.metric_i64("heap_after_expiry_bytes", after.heap.bytes);
    outcome.metric_u64("snapshot_after_expiry_bytes", after.snap.total_bytes);
    Ok(outcome)
}

/// One stream, `appends` appends 10 ms apart, cold flush keeps hot bounded.
/// Runs the identical workload with TTL none / sliding / absolute and reports
/// the heap difference, which is the node-local TTL index.
fn ttl_heap(args: &W5Args, sink: &mut Sink) -> Result<Outcome> {
    const HOT_FLUSH_BYTES: usize = 8 * smx::MIB;
    let appends = args.appends;
    let ttl = args.ttl_heap_seconds;
    sink.row(
        &json!({"mode": "ttl-heap", "appends": appends, "ttl_seconds": ttl,
        "retain_every": args.retain_every}),
    )?;
    let mut points: Vec<u64> = [10_000u64, 100_000, 1_000_000, 3_000_000]
        .into_iter()
        .filter(|c| *c <= appends)
        .collect();
    if points.last() != Some(&appends) {
        points.push(appends);
    }
    let mut outcome = Outcome::default();
    let mut finals = Vec::new();
    for variant in ["none", "sliding", "absolute"] {
        let mut rng = payload::Rng::new(23);
        // Allocated before the heap baseline, so it does not count as state.
        let mut starts = smx::RecordStarts::new(if args.retain_every > 0 { 1_002 } else { 0 });
        let base = Baseline::now();
        let mut m = StreamStateMachine::new();
        smx::create_bucket(&mut m, "bkt1")?;
        let id = smx::sid("bkt1", "h0001", "log");
        let (ttl_seconds, expires_at) = match variant {
            "sliding" => (Some(ttl), None),
            "absolute" => (None, Some(smx::T0.saturating_add(ttl.saturating_mul(1000)))),
            _ => (None, None),
        };
        smx::create_stream(&mut m, &id, ttl_seconds, expires_at, smx::T0)?;
        let mut packs = smx::PackPaths::default();
        let mut stats = smx::FlushStats::default();
        let mut n = 0u64;
        for cp in &points {
            while n < *cp {
                let now = smx::T0.saturating_add(n.saturating_mul(10));
                let record = payload::json_record(&mut rng, n, 200);
                starts.push(n, smx::tail(&m, &id));
                smx::ok(smx::append(&mut m, &id, record, None, now), "append")?;
                n = n.saturating_add(1);
                if m.total_hot_payload_bytes() >= HOT_FLUSH_BYTES as u64 {
                    smx::flush_pass(
                        &mut m,
                        HOT_FLUSH_BYTES,
                        HOT_FLUSH_BYTES,
                        &mut packs,
                        &mut stats,
                    )?;
                }
                if args.retain_every > 0
                    && n.is_multiple_of(args.retain_every)
                    && n > 1000
                    && let Some((_, offset)) = starts.at_or_below(n.saturating_sub(1000))
                    && offset > m.retained_offset(&id)
                {
                    smx::checkpoint_and_retain(&mut m, &id, offset, br#"{"c":1}"#, now)?;
                }
            }
            let heap = alloc::heap().saturating_sub(base.heap);
            let tight = alloc::tight_size(&m);
            let gauges = m.state_gauges();
            outcome.check(
                "f8_ttl_heap_entries",
                "TTL heap entries <= 2 x live TTL streams (F8)",
                gauges.ttl_heap_entries as f64,
                gauges.ttl_streams.saturating_mul(2) as f64,
            );
            sink.row(
                &json!({"variant": variant, "appends": n, "heap_actual_bytes": heap.bytes,
                "heap_actual_blocks": heap.blocks, "heap_tight_bytes": tight.bytes,
                "ttl_heap_entries": gauges.ttl_heap_entries, "ttl_streams": gauges.ttl_streams}),
            )?;
            if *cp == appends {
                finals.push((variant, heap.bytes, heap.blocks, gauges.ttl_heap_entries));
            }
        }
        smx::ok(
            m.apply(StreamCommand::DeleteStream {
                stream_id: id.clone(),
            }),
            "delete",
        )?;
        smx::ack_all_cold_gc(&mut m)?;
        let after_delete = alloc::heap().saturating_sub(base.heap);
        let gauges = m.state_gauges();
        sink.row(&json!({"variant": variant, "appends": n,
            "heap_after_delete_stream_bytes": after_delete.bytes,
            "ttl_heap_entries_after_delete": gauges.ttl_heap_entries}))?;
        outcome.metric_u64(
            &format!("{variant}.ttl_heap_entries_after_delete"),
            gauges.ttl_heap_entries,
        );
    }
    let Some(&(_, none_bytes, none_blocks, _)) = finals.iter().find(|f| f.0 == "none") else {
        bail!("missing baseline variant");
    };
    for (variant, bytes, blocks, entries) in &finals {
        outcome.metric_u64(&format!("{variant}.ttl_heap_entries"), *entries);
        if *variant == "none" {
            continue;
        }
        let per_append = round3(bytes.saturating_sub(none_bytes) as f64 / appends.max(1) as f64);
        outcome.metric(&format!("{variant}.ttl_index_bytes_per_append"), per_append);
        sink.row(&json!({"variant": variant, "appends": appends,
            "ttl_index_bytes_per_append": per_append,
            "ttl_index_blocks_per_append": round3(blocks.saturating_sub(none_blocks) as f64 / appends.max(1) as f64)}))?;
    }
    Ok(outcome)
}

fn purge(args: &W5Args, sink: &mut Sink) -> Result<Outcome> {
    let buckets = args.buckets;
    sink.row(&json!({"mode": "purge", "buckets": buckets}))?;
    let mut rng = payload::Rng::new(24);
    let base = Baseline::now();
    let mut m = StreamStateMachine::new();
    for b in 0..buckets {
        let bucket = format!("tenant-{b:08}");
        smx::create_bucket(&mut m, &bucket)?;
        let id = smx::sid(&bucket, "h1", "log");
        smx::create_stream(&mut m, &id, None, None, smx::T0)?;
        let record = payload::json_record(&mut rng, 0, 200);
        smx::ok(smx::append(&mut m, &id, record, None, smx::T0), "append")?;
        smx::ok(
            m.apply(StreamCommand::PurgeBucket { bucket_id: bucket }),
            "purge",
        )?;
    }
    let measured = measure_sm(&m, &base, Vec::new(), false)?;
    let heap = u64::try_from(measured.heap.bytes.max(0))?;
    sink.row(&json!({"phase": "after purging every bucket", "buckets_purged": buckets,
        "heap_bytes_per_purged_bucket": round3(ratio(heap, buckets as u64)),
        "snapshot_bytes_per_purged_bucket": round3(ratio(measured.snap.total_bytes, buckets as u64)),
        "m": measured.to_json()}))?;
    let mut outcome = Outcome::default();
    outcome.metric_i64("heap_bytes", measured.heap.bytes);
    outcome.metric_u64("snapshot_bytes", measured.snap.total_bytes);
    outcome.metric_u64("erased_buckets", measured.gauges.erased_buckets);
    outcome.metric_u64("bucket_usage_rows", measured.gauges.bucket_usage_rows);
    Ok(outcome)
}
