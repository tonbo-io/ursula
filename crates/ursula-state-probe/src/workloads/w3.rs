//! W3: a JSON stream that only receives external appends (1 MiB or more,
//! staged to S3). `--recs-per-append=K` models a large array body flattened
//! into K records. `--inline-every=N` adds one small inline append plus a flush
//! pass every N external appends. `--retain-every=N` (W6): checkpoint and
//! retention every N external appends, keeping the last `--retain-keep` records.
//! Runs at feature level 1, where external appends collapse message records
//! below the seal point (F4a). `--external-locators` runs at feature level 3
//! instead (F5): each external append keeps its locator in state, and the
//! workload models the leader's offload pass after every append, offloading a
//! stream's staged refs once it holds more than T_ext = 16 or one is 10 s old.

use std::collections::HashMap;

use anyhow::Result;
use clap::Args;
use serde_json::json;
use ursula_stream::StreamCommand;
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
pub struct W3Args {
    #[arg(long, default_value_t = 10_000)]
    pub appends: u64,
    #[arg(long, default_value_t = 1.0)]
    pub payload_mib: f64,
    #[arg(long, default_value_t = 1)]
    pub recs_per_append: u64,
    #[arg(long, default_value_t = 0)]
    pub inline_every: u64,
    #[arg(long, default_value_t = 0)]
    pub retain_every: u64,
    #[arg(long, default_value_t = 1_000)]
    pub retain_keep: u64,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "10,100,1000,10000,100000"
    )]
    pub checkpoints: Vec<u64>,
    #[arg(long)]
    pub zstd: bool,
    /// Run at feature level 3 (F5) and model the offload pass.
    #[arg(long)]
    pub external_locators: bool,
    #[arg(long)]
    pub name: Option<String>,
}

pub fn default_name(args: &W3Args) -> String {
    let recs = args.recs_per_append;
    args.name.clone().unwrap_or_else(|| {
        if args.external_locators {
            format!("w3_lb3_external_r{recs}")
        } else if args.retain_every > 0 {
            format!("w6_w3_retention_r{recs}")
        } else if args.inline_every > 0 {
            format!("w3_external_r{recs}_inline{}", args.inline_every)
        } else {
            format!("w3_external_r{recs}")
        }
    })
}

pub fn run(args: &W3Args, sink: &mut Sink) -> Result<Outcome> {
    let name = default_name(args);
    let payload_bytes = (args.payload_mib * smx::MIB as f64) as u64;
    let recs = args.recs_per_append.max(1);
    sink.row(
        &json!({"workload": name, "appends": args.appends, "payload_bytes": payload_bytes,
        "recs_per_append": recs, "inline_every": args.inline_every,
        "retain_every": args.retain_every, "retain_keep": args.retain_keep}),
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
    let ends: Vec<u64> = (1..=recs).map(|k| k * payload_bytes / recs).collect();
    let mut rng = payload::Rng::new(5);
    let base = Baseline::now();
    let mut m = StreamStateMachine::new();
    let level = if args.external_locators {
        ursula_stream::FEATURE_LEVEL_EXTERNAL_LOCATORS
    } else {
        ursula_stream::FEATURE_LEVEL_KEYED_STREAMS
    };
    smx::raise_feature_level(&mut m, level)?;
    let mut staged_at: HashMap<String, u64> = HashMap::new();
    let mut max_staged = 0_u64;
    let mut offloads = 0_u64;
    smx::create_bucket(&mut m, "bkt1")?;
    let id = smx::sid("bkt1", "h0001", "log");
    smx::create_stream(&mut m, &id, None, None, smx::T0)?;
    let mut packs = smx::PackPaths::default();
    let mut stats = smx::FlushStats::default();
    let mut outcome = Outcome::default();
    let mut n = 0u64;
    let mut inline_appends = 0u64;
    let mut records = 0u64;
    let mut retentions = 0u64;
    let checkpoint_payload = br#"{"checkpoint":1}"#;
    for cp in points {
        while n < cp {
            let now = smx::T0 + n * 1000;
            smx::ok(
                smx::append_external(&mut m, &id, payload_bytes, ends.clone(), now),
                "append external",
            )?;
            if args.external_locators {
                for object in m.external_segments(&id) {
                    staged_at.entry(object.s3_path.clone()).or_insert(now);
                }
                max_staged = max_staged.max(m.external_segments(&id).len() as u64);
                offloads += offload_pass(&mut m, &mut staged_at, now)?;
            }
            n += 1;
            records += recs;
            if args.inline_every > 0 && n.is_multiple_of(args.inline_every) {
                let record = payload::json_record(&mut rng, records, 200);
                smx::ok(smx::append(&mut m, &id, record, None, now), "inline append")?;
                inline_appends += 1;
                records += 1;
                smx::flush_pass(&mut m, 1, 8 * smx::MIB, &mut packs, &mut stats)?;
            }
            if args.retain_every > 0
                && n.is_multiple_of(args.retain_every)
                && records > args.retain_keep
            {
                smx::checkpoint_and_retain(
                    &mut m,
                    &id,
                    records - args.retain_keep,
                    checkpoint_payload,
                    now,
                )?;
                retentions += 1;
            }
        }
        let total_appends = n + inline_appends;
        let measured = measure_sm(
            &m,
            &base,
            smx::append_counts(std::slice::from_ref(&id), &[total_appends]),
            args.zstd,
        )?;
        formula::per_stream_checks(&mut outcome, &measured, 0);
        let heap = u64::try_from(measured.heap.bytes.max(0))?;
        sink.row(&json!({
            "workload": name,
            "external_appends": n,
            "inline_appends": inline_appends,
            "records": records,
            "logical_bytes": m.head(&id).map(|h| h.tail_offset),
            "external_segments_in_state": m.external_segments(&id).len(),
            "max_staged_external_refs": max_staged,
            "offloads": offloads,
            "cold_refs_in_state": m.cold_chunks(&id).len(),
            "hot_bytes": m.hot_payload_len(&id).unwrap_or(0),
            "flush": stats,
            "retentions": retentions,
            "per_external_append": {
                "heap_bytes": round3(ratio(heap, n)),
                "snapshot_bytes": round3(ratio(measured.snap.total_bytes, n)),
            },
            "m": measured.to_json(),
        }))?;
        if n == args.appends {
            outcome.metric_u64("records", records);
            outcome.metric_i64("heap_bytes", measured.heap.bytes);
            outcome.metric_u64("snapshot_bytes", measured.snap.total_bytes);
            outcome.metric_u64("message_records", measured.gauges.message_records);
            outcome.metric_u64(
                "snapshot_message_records_bytes",
                measured.snap.message_records_bytes,
            );
            outcome.metric_u64("dense_entries", measured.gauges.dense_record_entries);
            outcome.metric_u64("staged_external_refs", measured.gauges.staged_external_refs);
            if args.external_locators {
                outcome.metric_u64("max_staged_external_refs", max_staged);
                outcome.check(
                    "f5_max_staged_external_refs",
                    "staged external refs per stream stay <= 16 at every append (F5)",
                    max_staged as f64,
                    ursula_stream::MAX_STAGED_EXTERNAL_REFS as f64,
                );
            }
        }
    }
    Ok(outcome)
}

/// The leader's offload pass (F5), applied as the engines do after writing
/// the refs' page entries: every stream holding more than T_ext staged refs,
/// or one staged at least 10 s (simulated) ago, proposes `OffloadColdRefs`
/// for all of its refs. Returns the commands applied.
fn offload_pass(
    m: &mut StreamStateMachine,
    staged_at: &mut HashMap<String, u64>,
    now_ms: u64,
) -> Result<u64> {
    let candidates = m.staged_external_ref_candidates(
        ursula_stream::MAX_STAGED_EXTERNAL_REFS,
        &|object| {
            staged_at.get(&object.s3_path).is_none_or(|at| {
                now_ms.saturating_sub(*at) >= ursula_stream::STAGED_EXTERNAL_REF_MAX_AGE_MS
            })
        },
        64,
    );
    let mut applied = 0;
    for candidate in candidates {
        for object in &candidate.refs {
            staged_at.remove(&object.s3_path);
        }
        smx::ok(
            m.apply(StreamCommand::OffloadColdRefs {
                stream_id: candidate.stream_id,
                refs: candidate.refs,
            }),
            "offload cold refs",
        )?;
        applied += 1;
    }
    Ok(applied)
}
