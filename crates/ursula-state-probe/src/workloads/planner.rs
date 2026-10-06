//! Cost of one cold-flush planning pass (`plan_next_cold_flush_batch`, which
//! the leader's flush worker calls every second per group) against the number
//! of streams holding hot bytes, plus the W2 starved-stream shape. Reports
//! deterministic counters (candidates, bytes, whether the starved stream is
//! included) next to wall time.
#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]

use std::time::Instant;

use anyhow::Result;
use clap::Args;
use serde_json::json;
use ursula_stream::StreamStateMachine;

use crate::out::Outcome;
use crate::out::Sink;
use crate::out::round3;
use crate::payload;
use crate::smx;

#[derive(Debug, Clone, Args)]
pub struct PlannerArgs {
    /// Stream counts for the drain-pass case.
    #[arg(long, value_delimiter = ',', default_value = "10,100,300,1000,3000")]
    pub streams: Vec<usize>,
    /// Hot MiB held by the starved stream in the W2-shape case.
    #[arg(long, default_value_t = 9)]
    pub starved_mib: usize,
    #[arg(long)]
    pub name: Option<String>,
}

fn build(
    streams: usize,
    appends: usize,
    rec: usize,
    starved_bytes: usize,
) -> Result<StreamStateMachine> {
    let mut rng = payload::Rng::new(31);
    let mut m = StreamStateMachine::new();
    smx::create_bucket(&mut m, "bkt1")?;
    for i in 0..streams {
        let id = smx::sid("bkt1", &format!("h{i:06}"), &format!("log{i:06}"));
        smx::create_stream(&mut m, &id, None, None, smx::T0)?;
        for k in 0..appends {
            let record = payload::json_record(&mut rng, k as u64, rec);
            smx::ok(smx::append(&mut m, &id, record, None, smx::T0), "append")?;
        }
    }
    if starved_bytes > 0 {
        let id = smx::sid("bkt1", "zz-starved", "zz-starved-log");
        smx::create_stream(&mut m, &id, None, None, smx::T0)?;
        let mut held = 0;
        while held < starved_bytes {
            let record = payload::json_record(&mut rng, held as u64, 300);
            held += record.len();
            smx::ok(smx::append(&mut m, &id, record, None, smx::T0), "append")?;
        }
    }
    Ok(m)
}

fn time_plan(
    m: &StreamStateMachine,
    min_hot: usize,
    max_flush: usize,
    reps: usize,
) -> Result<(f64, usize, u64)> {
    let started = Instant::now();
    let mut count = 0;
    let mut bytes = 0u64;
    for _ in 0..reps.max(1) {
        let candidates = m
            .plan_next_cold_flush_batch(min_hot, max_flush, max_flush, 4096)
            .map_err(|err| anyhow::anyhow!("plan: {err:?}"))?;
        count = candidates.len();
        bytes = candidates.iter().map(|c| c.payload.len() as u64).sum();
    }
    Ok((
        started.elapsed().as_secs_f64() * 1e3 / reps.max(1) as f64,
        count,
        bytes,
    ))
}

pub fn run(args: &PlannerArgs, sink: &mut Sink) -> Result<Outcome> {
    let mut outcome = Outcome::default();
    for &streams in &args.streams {
        let m = build(streams, 5, 200, 0)?;
        let reps = if streams <= 300 { 20 } else { 1 };
        let (ms, count, bytes) = time_plan(&m, 1, 8 * smx::MIB, reps)?;
        sink.row(
            &json!({"case": "drain pass, every stream has 5 hot records",
            "streams_with_hot": streams, "plan_ms": round3(ms), "candidates": count,
            "candidate_bytes": bytes}),
        )?;
        outcome.metric_u64(&format!("drain_{streams}.candidates"), count as u64);
        outcome.metric_u64(&format!("drain_{streams}.candidate_bytes"), bytes);
    }
    let starved = args.starved_mib * smx::MIB;
    let m = build(200, 1, 300, starved)?;
    let (ms, count, bytes) = time_plan(&m, 8 * smx::MIB, 8 * smx::MIB, 5)?;
    let included = bytes as usize > starved;
    sink.row(
        &json!({"case": "W2 shape: 200 trickle streams (1 hot record) + starved stream",
        "starved_stream_hot_mib": args.starved_mib, "plan_ms": round3(ms), "candidates": count,
        "candidate_bytes": bytes, "starved_included": included}),
    )?;
    outcome.metric_u64("starved.candidates", count as u64);
    outcome.check(
        "f10_starved_stream_planned",
        "a stream above flush_size is planned despite smaller streams (F10); value 1 = starved",
        if included { 0.0 } else { 1.0 },
        0.0,
    );
    Ok(outcome)
}
