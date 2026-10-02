//! W2: one Raft group of N slow-trickle JSON streams sharing packed flushes.
//! Simulated 1 s ticks: every stream appends with probability `rate` per tick;
//! the cold flush worker pass runs every tick as `ShardRuntime` does (group
//! threshold 8 MiB, then drain every stream with hot bytes into one pack per
//! bucket).
//!
//! `--pressure-groups=G` models the node-level pressure flush (aggregate hot of
//! at least 128 MiB over G equally loaded led groups drains every stream).
//! `--retain-every-sec=S` (W6): every S simulated seconds each stream
//! publishes a checkpoint and retains only its last `--retain-keep` records.
//! `--compact` runs the existing `CompactCold` (shared to exclusive) for every
//! stream at the end.

use std::collections::BTreeMap;
use std::collections::HashMap;

use anyhow::Result;
use clap::Args;
use serde_json::json;
use ursula_shard::BucketStreamId;
use ursula_stream::ColdChunkRef;
use ursula_stream::StreamCommand;
use ursula_stream::StreamResponse;
use ursula_stream::StreamStateMachine;

use crate::formula;
use crate::out::Baseline;
use crate::out::Outcome;
use crate::out::Sink;
use crate::out::measure_sm;
use crate::out::round3;
use crate::payload;
use crate::smx;

/// Refs one stream can gain between two runs of the (future) F2 compaction
/// driver: one per flush tick over a 60 s driver interval.
const DRIVER_INTERVAL_REFS: u64 = 60;

#[derive(Debug, Clone, Args)]
pub struct W2Args {
    #[arg(long, default_value_t = 200)]
    pub streams: usize,
    #[arg(long, default_value_t = 24.0)]
    pub hours: f64,
    /// Append probability per stream per simulated second.
    #[arg(long, default_value_t = 0.5)]
    pub rate: f64,
    #[arg(long, default_value_t = 300)]
    pub rec_bytes: usize,
    /// Model the node pressure flush over this many equally loaded groups.
    #[arg(long, default_value_t = 0)]
    pub pressure_groups: usize,
    #[arg(long, default_value_t = 0)]
    pub retain_every_sec: u64,
    #[arg(long, default_value_t = 500)]
    pub retain_keep: u64,
    /// `flush_max_size` and batch size, MiB (default config: 8).
    #[arg(long, default_value_t = 8)]
    pub max_flush_mib: usize,
    #[arg(long)]
    pub compact: bool,
    #[arg(long, default_value_t = 1.0)]
    pub measure_every_h: f64,
    #[arg(long)]
    pub zstd: bool,
    #[arg(long)]
    pub name: Option<String>,
}

pub fn default_name(args: &W2Args) -> String {
    args.name.clone().unwrap_or_else(|| {
        if args.retain_every_sec > 0 {
            "w6_w2_retention".to_owned()
        } else if args.pressure_groups > 0 {
            format!("w2_packs_pressure{}", args.pressure_groups)
        } else {
            "w2_packs".to_owned()
        }
    })
}

fn percentile(sorted: &[u64], numerator: usize, denominator: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1) * numerator / denominator.max(1);
    sorted.get(index).copied().unwrap_or(0)
}

pub fn run(args: &W2Args, sink: &mut Sink) -> Result<Outcome> {
    let name = default_name(args);
    let flush_bytes = 8 * smx::MIB;
    let max_flush_bytes = args.max_flush_mib * smx::MIB;
    let pressure_bytes = 128 * smx::MIB as u64;
    sink.row(
        &json!({"workload": name, "streams": args.streams, "hours": args.hours,
        "rate_per_stream_per_s": args.rate, "rec_bytes": args.rec_bytes,
        "pressure_groups": args.pressure_groups, "retain_every_sec": args.retain_every_sec,
        "retain_keep": args.retain_keep, "group_flush_threshold_bytes": flush_bytes,
        "max_flush_and_batch_bytes": max_flush_bytes}),
    )?;

    let mut rng = payload::Rng::new(7);
    let base = Baseline::now();
    let mut m = StreamStateMachine::new();
    smx::create_bucket(&mut m, "bkt1")?;
    // Distinct names: deterministic planner order (§7.1).
    let ids: Vec<BucketStreamId> = (0..args.streams)
        .map(|i| smx::sid("bkt1", &format!("h{i:05}"), &format!("log{i:05}")))
        .collect();
    for id in &ids {
        smx::create_stream(&mut m, id, None, None, smx::T0)?;
    }
    let index_of: HashMap<BucketStreamId, usize> = ids
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, id)| (id, i))
        .collect();
    let mut appends = vec![0u64; args.streams];
    let mut packs = smx::PackPaths::default();
    let mut stats = smx::FlushStats::default();
    let mut refs: BTreeMap<usize, Vec<ColdChunkRef>> = BTreeMap::new();
    let mut outcome = Outcome::default();
    let total_secs = (args.hours * 3600.0) as u64;
    let measure_every = ((args.measure_every_h * 3600.0) as u64).max(1);
    let threshold = (args.rate.max(1e-9) * 1e6) as u64;
    let checkpoint_payload = br#"{"checkpoint":1}"#;
    let mut retentions = 0u64;
    let mut flush_ns_interval: u64 = 0;
    let mut max_stream_hot_seen = 0u64;
    let mut first_starved_sec: Option<u64> = None;
    let mut refs_added_interval = 0u64;
    for sec in 1..=total_secs {
        let now = smx::T0 + sec * 1000;
        for (i, id) in ids.iter().enumerate() {
            if rng.below(1_000_000) < threshold {
                let seq = appends.get(i).copied().unwrap_or(0);
                let record = payload::json_record(&mut rng, seq, args.rec_bytes);
                smx::ok(smx::append(&mut m, id, record, None, now), "append")?;
                if let Some(count) = appends.get_mut(i) {
                    *count += 1;
                }
            }
        }
        let group_hot = m.total_hot_payload_bytes();
        let min_hot = if args.pressure_groups > 0
            && group_hot * args.pressure_groups as u64 >= pressure_bytes
        {
            1
        } else {
            flush_bytes
        };
        let started = std::time::Instant::now();
        let published = smx::flush_pass(&mut m, min_hot, max_flush_bytes, &mut packs, &mut stats)?;
        flush_ns_interval += u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        for (stream_id, chunk) in published {
            if chunk.shared_object {
                refs_added_interval += 1;
                if let Some(index) = index_of.get(&stream_id) {
                    refs.entry(*index).or_default().push(chunk);
                }
            }
        }
        if args.retain_every_sec > 0 && sec % args.retain_every_sec == 0 {
            for (i, id) in ids.iter().enumerate() {
                let range = m
                    .record_range(id)
                    .map_err(|err| anyhow::anyhow!("record range: {err:?}"))?;
                let next = range.map_or(0, |r| r.next_record);
                if next > args.retain_keep {
                    smx::checkpoint_and_retain(
                        &mut m,
                        id,
                        next - args.retain_keep,
                        checkpoint_payload,
                        now,
                    )?;
                    retentions += 1;
                }
                let live = m.cold_chunks(id).len();
                if let Some(mirror) = refs.get_mut(&i) {
                    let dropped = mirror.len().saturating_sub(live);
                    mirror.drain(..dropped);
                }
            }
        }
        // Starvation: one stream's hot bytes above 2 x flush_size (F10).
        // Sampled once a simulated minute: `hot_payload_len` scans chunks.
        let max_hot = if sec % 60 != 0 && sec != total_secs {
            0
        } else {
            ids.iter()
                .map(|id| m.hot_payload_len(id).unwrap_or(0))
                .max()
                .unwrap_or(0)
        };
        max_stream_hot_seen = max_stream_hot_seen.max(max_hot);
        if first_starved_sec.is_none() && max_hot > 2 * flush_bytes as u64 {
            first_starved_sec = Some(sec);
        }
        if sec % measure_every == 0 || sec == total_secs {
            let measured = measure_sm(&m, &base, smx::append_counts(&ids, &appends), args.zstd)?;
            formula::per_stream_checks(&mut outcome, &measured, DRIVER_INTERVAL_REFS);
            let mut ref_counts: Vec<u64> = measured
                .snap
                .streams
                .iter()
                .map(|s| s.shared_refs)
                .collect();
            ref_counts.sort_unstable();
            let mut hots: Vec<u64> = measured.snap.streams.iter().map(|s| s.hot_bytes).collect();
            hots.sort_unstable();
            sink.row(&json!({
                "workload": name,
                "sim_hours": round3(sec as f64 / 3600.0),
                "appends_total": appends.iter().sum::<u64>(),
                "flush": stats,
                "retentions": retentions,
                "flush_worker_cpu_ms_per_sim_hour": round3(flush_ns_interval as f64 / 1e6 / (measure_every as f64 / 3600.0)),
                "hot": {"group_bytes": m.total_hot_payload_bytes(), "max_stream_bytes": percentile(&hots, 1, 1), "median_stream_bytes": percentile(&hots, 1, 2)},
                "refs_per_stream": {"min": percentile(&ref_counts, 0, 1), "median": percentile(&ref_counts, 1, 2), "max": percentile(&ref_counts, 1, 1)},
                "refs_added_this_interval": refs_added_interval,
                "live_packs_in_group_maps": measured.gauges.live_packs,
                "pending_cold_gc": m.pending_cold_gc_len(),
                "first_starved_sim_sec": first_starved_sec,
                "m": measured.to_json(),
            }))?;
            flush_ns_interval = 0;
            refs_added_interval = 0;
            if sec == total_secs {
                outcome.metric_u64("appends", appends.iter().sum());
                outcome.metric_u64("flush_passes", stats.passes);
                outcome.metric_u64("packs", stats.packs);
                outcome.metric_u64("pack_slices", stats.pack_slices);
                outcome.metric_u64("max_shared_refs_per_stream", percentile(&ref_counts, 1, 1));
                outcome.metric_u64("live_packs", measured.gauges.live_packs);
                outcome.metric_u64("snapshot_bytes", measured.snap.total_bytes);
                outcome.metric_u64("snapshot_cold_chunk_bytes", measured.snap.cold_chunks_bytes);
                outcome.metric_i64("heap_bytes", measured.heap.bytes);
                outcome.metric_u64("max_stream_hot_bytes_seen", max_stream_hot_seen);
            }
        }
    }
    outcome.check(
        "f10_max_stream_hot",
        "max stream hot bytes <= 2 x flush_size (F10, starvation)",
        max_stream_hot_seen as f64,
        (2 * flush_bytes) as f64,
    );

    if args.compact {
        compact_all(
            &mut m, &ids, &mut refs, total_secs, &base, &appends, args.zstd, &name, sink,
        )?;
    }
    Ok(outcome)
}

fn compact_all(
    m: &mut StreamStateMachine,
    ids: &[BucketStreamId],
    refs: &mut BTreeMap<usize, Vec<ColdChunkRef>>,
    total_secs: u64,
    base: &Baseline,
    appends: &[u64],
    zstd: bool,
    name: &str,
    sink: &mut Sink,
) -> Result<()> {
    let before: usize = refs.values().map(Vec::len).sum();
    let mut ok_count = 0;
    let mut err_count = 0;
    let mut first_err = None;
    for (index, old) in std::mem::take(refs) {
        let (Some(first), Some(last), Some(id)) = (old.first(), old.last(), ids.get(index)) else {
            continue;
        };
        let (start, end) = (first.start_offset, last.end_offset);
        let replacement = ColdChunkRef {
            start_offset: start,
            end_offset: end,
            s3_path: ursula_runtime::new_cold_chunk_path(id, start, end),
            object_size: end - start,
            object_offset: 0,
            shared_object: false,
            payload_digest: String::new(),
        };
        match m.apply(StreamCommand::CompactCold {
            stream_id: id.clone(),
            old_chunks: old,
            replacement,
            gc_not_before_ms: smx::T0 + total_secs * 1000 + 300_000,
        }) {
            StreamResponse::ColdCompacted { .. } => ok_count += 1,
            other => {
                err_count += 1;
                if first_err.is_none() {
                    first_err = Some(format!("{other:?}"));
                }
            }
        }
    }
    let measured = measure_sm(m, base, smx::append_counts(ids, appends), zstd)?;
    sink.row(&json!({
        "workload": name,
        "phase": "after CompactCold(shared->exclusive) for every stream",
        "refs_before": before,
        "compactions_ok": ok_count,
        "compactions_err": err_count,
        "first_err": first_err,
        "pending_cold_gc": m.pending_cold_gc_len(),
        "m": measured.to_json(),
    }))?;
    smx::ack_all_cold_gc(m)?;
    let measured = measure_sm(m, base, smx::append_counts(ids, appends), zstd)?;
    sink.row(&json!({
        "workload": name,
        "phase": "after AckColdGc (GC worker drained the queue)",
        "pending_cold_gc": m.pending_cold_gc_len(),
        "m": measured.to_json(),
    }))?;
    Ok(())
}
