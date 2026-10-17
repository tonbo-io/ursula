//! L2 cross-checks through the real `ShardRuntime` (core workers, group
//! actors, real cold-flush orchestration with packing, memory cold store) on
//! the in-memory engine and the single-node OpenRaft engine
//! (`DurableRaftGroupEngineFactory`, on a per-core journal in a temporary
//! directory), then the group snapshot encoded with the real codec.
//!
//! - `w1`: one stream, inline appends, a flush worker pass every tick.
//! - `w2`: N trickle streams in one group, a flush worker pass every tick.
//! - `compact`: `CompactCold` of shared pack slices on both engines.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use clap::Args;
use clap::ValueEnum;
use serde_json::Value;
use serde_json::json;
use ursula_config::WalFsync;
use ursula_raft::DurableRaftGroupEngineFactory;
use ursula_raft::RaftWal;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdChunkRef;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreHandle;
use ursula_runtime::CompactColdRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::InMemoryGroupEngineFactory;
use ursula_runtime::PlanGroupColdFlushRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use crate::codec;
use crate::codec::SnapStats;
use crate::out::Outcome;
use crate::out::Sink;
use crate::out::round3;
use crate::payload;

const T0: u64 = 1_759_300_000_000;
const MIB: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum L2Mode {
    W1,
    W2,
    Compact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Engine {
    Memory,
    Raft,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Engine::Memory => "memory",
            Engine::Raft => "raft",
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct L2Args {
    #[arg(long, value_enum, default_value = "w1")]
    pub mode: L2Mode,
    #[arg(long, value_enum, default_value = "memory")]
    pub engine: Engine,
    /// `w1`: records.
    #[arg(long, default_value_t = 100_000)]
    pub records: u64,
    #[arg(long, default_value_t = 200)]
    pub rec_bytes: usize,
    /// `w1`: appends per flush tick.
    #[arg(long, default_value_t = 100)]
    pub per_tick: u64,
    /// Per-group hot admission cap, MiB (0 = unlimited).
    #[arg(long, default_value_t = 0)]
    pub admission_mib: u64,
    /// `w2`: streams.
    #[arg(long, default_value_t = 50)]
    pub streams: usize,
    /// `w2`: append probability per stream per simulated second.
    #[arg(long, default_value_t = 1.0)]
    pub rate: f64,
    /// `w2`: bytes per record.
    #[arg(long, default_value_t = 2000)]
    pub w2_rec_bytes: usize,
    /// `w2`: simulated minutes.
    #[arg(long, default_value_t = 120)]
    pub minutes: u64,
    #[arg(long, default_value_t = 8)]
    pub max_flush_mib: usize,
    #[arg(long, default_value_t = 10)]
    pub measure_every_min: u64,
    #[arg(long)]
    pub name: Option<String>,
}

pub fn default_name(args: &L2Args) -> String {
    args.name.clone().unwrap_or_else(|| match args.mode {
        L2Mode::W1 => format!("l2_w1_{}", args.engine.label()),
        L2Mode::W2 => format!("l2_w2_{}", args.engine.label()),
        L2Mode::Compact => "l2_compact_both_engines".to_owned(),
    })
}

/// The Raft engine's WAL in a temporary directory.
struct TempWal {
    log_stores: RaftWal,
    root: PathBuf,
}

/// Stops the Raft groups of `rt` and closes its WAL as the server does, then
/// removes the WAL directory. Until then a core writer may still write to it,
/// and a journal write that fails stops the process, so an error return
/// leaves the directory behind.
async fn shutdown(rt: &ShardRuntime, wal: Option<TempWal>) -> Result<()> {
    let Some(wal) = wal else {
        return Ok(());
    };
    rt.shutdown_group_engines()
        .await
        .map_err(|err| anyhow::anyhow!("stop the Raft groups: {err}"))?;
    wal.log_stores
        .shutdown()
        .await
        .context("shut the Raft WAL down")?;
    std::fs::remove_dir_all(&wal.root).context("remove the Raft WAL directory")
}

/// Spawns a runtime of `engine`. The Raft engine's journals live in the
/// returned temporary WAL; pass it to [`shutdown`] once the runtime is done.
fn spawn(
    engine: Engine,
    cold: ColdStoreHandle,
    admission: Option<u64>,
) -> Result<(ShardRuntime, Option<TempWal>)> {
    let config = RuntimeConfig::new(1, 1).with_cold_max_hot_bytes_per_group(admission);
    let (runtime, wal) = match engine {
        Engine::Raft => {
            let root = tempfile::tempdir()
                .context("create the Raft WAL directory")?
                .keep();
            let log_stores = RaftWal::start(
                &root,
                WalFsync::Never,
                &ursula_shard::StaticShardMap::new(1, 1)?,
            )
            .context("start the Raft WAL")?;
            (
                ShardRuntime::spawn_with_engine_factory_and_cold_store(
                    config,
                    DurableRaftGroupEngineFactory::with_cold_store(
                        log_stores.clone(),
                        Some(cold.clone()),
                    ),
                    Some(cold),
                ),
                Some(TempWal { log_stores, root }),
            )
        }
        Engine::Memory => (
            ShardRuntime::spawn_with_engine_factory_and_cold_store(
                config,
                InMemoryGroupEngineFactory::with_cold_store(Some(cold.clone())),
                Some(cold),
            ),
            None,
        ),
    };
    let runtime = runtime.map_err(|err| anyhow::anyhow!("spawn runtime: {err}"))?;
    Ok((runtime, wal))
}

async fn create(rt: &ShardRuntime, id: &BucketStreamId) -> Result<()> {
    let mut request = CreateStreamRequest::new(id.clone(), "application/json");
    request.now_ms = T0;
    rt.create_stream(request)
        .await
        .map_err(|err| anyhow::anyhow!("create stream: {err}"))?;
    Ok(())
}

async fn append(
    rt: &ShardRuntime,
    id: &BucketStreamId,
    payload: Vec<u8>,
    now_ms: u64,
) -> Result<()> {
    rt.append(AppendRequest {
        stream_id: id.clone(),
        content_type: "application/json".to_owned(),
        payload: bytes::Bytes::from(payload),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms,
        if_incarnation: None,
    })
    .await
    .map_err(|err| anyhow::anyhow!("append: {err}"))?;
    Ok(())
}

fn flush_request(min_hot: usize, max_flush: usize) -> PlanGroupColdFlushRequest {
    PlanGroupColdFlushRequest {
        min_hot_bytes: min_hot,
        max_flush_bytes: max_flush,
        max_batch_bytes: max_flush,
        pressure: None,
        max_hot_age: None,
    }
}

async fn snap(rt: &ShardRuntime) -> Result<(GroupSnapshot, SnapStats)> {
    let snapshot = rt
        .snapshot_group(RaftGroupId(0))
        .await
        .map_err(|err| anyhow::anyhow!("snapshot group: {err}"))?;
    let stats = codec::measure(snapshot.clone(), false)?;
    Ok((snapshot, stats))
}

/// Run one L2 mode on its own multi-threaded tokio runtime.
pub fn run(args: &L2Args, sink: &mut Sink) -> Result<Outcome> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    runtime.block_on(async {
        match args.mode {
            L2Mode::W1 => w1(args, sink).await,
            L2Mode::W2 => w2(args, sink).await,
            L2Mode::Compact => compact(sink).await,
        }
    })
}

async fn w1(args: &L2Args, sink: &mut Sink) -> Result<Outcome> {
    let admission = (args.admission_mib > 0).then_some(args.admission_mib << 20);
    let cold: ColdStoreHandle = Arc::new(ColdStore::memory().context("memory cold store")?);
    let (rt, wal) = spawn(args.engine, cold, admission)?;
    let id = BucketStreamId::new("bkt1", "h0001-log");
    create(&rt, &id).await?;
    let mut rng = payload::Rng::new(1);
    let started = Instant::now();
    let mut n = 0u64;
    let mut flushed = 0usize;
    let mut outcome = Outcome::default();
    let mut points: Vec<u64> = [1_000u64, 3_000, 10_000, 30_000, 100_000, 1_000_000]
        .into_iter()
        .filter(|c| *c <= args.records)
        .collect();
    if points.last() != Some(&args.records) {
        points.push(args.records);
    }
    for cp in points {
        while n < cp {
            append(
                &rt,
                &id,
                payload::json_record(&mut rng, n, args.rec_bytes),
                T0.saturating_add(n.saturating_mul(10)),
            )
            .await?;
            n = n.saturating_add(1);
            if n.is_multiple_of(args.per_tick.max(1)) {
                let flushed_now = rt
                    .flush_cold_all_groups_once_bounded(flush_request(8 * MIB, 8 * MIB), 4)
                    .await
                    .map_err(|err| anyhow::anyhow!("flush pass: {err}"))?;
                flushed = flushed.saturating_add(flushed_now);
            }
        }
        let (snapshot, stats) = snap(&rt).await?;
        let entry = snapshot.stream_snapshot.streams.first();
        let cold_chunks = entry.map_or(0, |e| e.cold_chunks.len());
        sink.row(&json!({
            "mode": "w1", "engine": args.engine.label(), "admission_mib": args.admission_mib,
            "records": n, "flushes": flushed,
            "elapsed_s": round3(started.elapsed().as_secs_f64()),
            "cold_chunks": cold_chunks, "group_commit_index": snapshot.group_commit_index,
            "snapshot": stats,
        }))?;
        if n == args.records {
            outcome.metric_u64("snapshot_bytes", stats.total_bytes);
            outcome.metric_u64("flushes", flushed as u64);
        }
    }
    shutdown(&rt, wal).await?;
    Ok(outcome)
}

#[expect(
    clippy::cast_sign_loss,
    reason = "probe arguments are non-negative rates and durations; `as` saturates anything else to zero"
)]
async fn w2(args: &L2Args, sink: &mut Sink) -> Result<Outcome> {
    let cold: ColdStoreHandle = Arc::new(ColdStore::memory().context("memory cold store")?);
    let (rt, wal) = spawn(args.engine, cold, None)?;
    let ids: Vec<_> = (0..args.streams)
        .map(|i| BucketStreamId::new("bkt1", format!("h{i:05}-log{i:05}")))
        .collect();
    for id in &ids {
        create(&rt, id).await?;
    }
    sink.row(
        &json!({"mode": "w2", "engine": args.engine.label(), "streams": args.streams,
        "rate": args.rate, "rec_bytes": args.w2_rec_bytes, "minutes": args.minutes,
        "max_flush_mib": args.max_flush_mib}),
    )?;
    let mut rng = payload::Rng::new(9);
    let mut seq = vec![0u64; args.streams];
    let threshold = (args.rate * 1e6) as u64;
    let mut passes_with_flush = 0u64;
    let mut slices = 0usize;
    let mut max_hot_seen = 0u64;
    let started = Instant::now();
    let mut outcome = Outcome::default();
    let max_flush_bytes = args
        .max_flush_mib
        .checked_mul(MIB)
        .context("--max-flush-mib overflows usize")?;
    let measure_every_secs = args.measure_every_min.max(1).saturating_mul(60);
    for sec in 1..=args.minutes.saturating_mul(60) {
        let now = T0.saturating_add(sec.saturating_mul(1000));
        for (id, s) in ids.iter().zip(seq.iter_mut()) {
            if rng.below(1_000_000) < threshold {
                append(
                    &rt,
                    id,
                    payload::json_record(&mut rng, *s, args.w2_rec_bytes),
                    now,
                )
                .await?;
                *s = s.saturating_add(1);
            }
        }
        let flushed = rt
            .flush_cold_all_groups_once_bounded(flush_request(8 * MIB, max_flush_bytes), 4)
            .await
            .map_err(|err| anyhow::anyhow!("flush pass: {err}"))?;
        if flushed > 0 {
            passes_with_flush = passes_with_flush.saturating_add(1);
            slices = slices.saturating_add(flushed);
        }
        if sec.checked_rem(measure_every_secs) == Some(0) {
            let (_, stats) = snap(&rt).await?;
            let max_refs = stats
                .streams
                .iter()
                .map(|s| s.shared_refs)
                .max()
                .unwrap_or(0);
            let max_hot = stats.streams.iter().map(|s| s.hot_bytes).max().unwrap_or(0);
            max_hot_seen = max_hot_seen.max(max_hot);
            sink.row(&json!({
                "mode": "w2", "engine": args.engine.label(), "sim_minutes": sec / 60,
                "passes_with_flush": passes_with_flush, "flushed_slices": slices,
                "max_shared_refs_per_stream": max_refs, "max_hot_per_stream": max_hot,
                "elapsed_s": round3(started.elapsed().as_secs_f64()),
                "snapshot": stats,
            }))?;
        }
    }
    outcome.metric_u64("passes_with_flush", passes_with_flush);
    outcome.metric_u64("flushed_slices", slices as u64);
    outcome.check(
        "f10_max_stream_hot",
        "max stream hot bytes <= 2 x flush_size (F10, starvation)",
        max_hot_seen as f64,
        (16 * MIB) as f64,
    );
    shutdown(&rt, wal).await?;
    Ok(outcome)
}

/// Force packed flushes of two streams, then compact one stream's shared
/// slices into an exclusive object with the existing `CompactCold`.
async fn compact_on(engine: Engine) -> Result<(Value, bool)> {
    let cold: ColdStoreHandle = Arc::new(ColdStore::memory().context("memory cold store")?);
    let (rt, wal) = spawn(engine, cold.clone(), None)?;
    let a = BucketStreamId::new("bkt1", "h1-log-a");
    let b = BucketStreamId::new("bkt1", "h2-log-b");
    create(&rt, &a).await?;
    create(&rt, &b).await?;
    let mut rng = payload::Rng::new(3);
    let mut flushed = 0usize;
    for round in 0..3u64 {
        for i in 0..10u64 {
            let seq = round.saturating_mul(10).saturating_add(i);
            let now = T0.saturating_add(i);
            append(&rt, &a, payload::json_record(&mut rng, seq, 200), now).await?;
            append(&rt, &b, payload::json_record(&mut rng, seq, 200), now).await?;
        }
        let flushed_now = rt
            .flush_cold_all_groups_once_bounded(flush_request(1, 8 * MIB), 1)
            .await
            .map_err(|err| anyhow::anyhow!("flush: {err}"))?;
        flushed = flushed.saturating_add(flushed_now);
    }
    let (snapshot, _) = snap(&rt).await?;
    let (generation, old): (u64, Vec<ColdChunkRef>) = snapshot
        .stream_snapshot
        .streams
        .iter()
        .find(|e| e.metadata.stream_id == a)
        .map(|e| (e.cold_index_generation, e.cold_chunks.clone()))
        .context("stream a in snapshot")?;
    let shared = old.iter().filter(|c| c.shared_object).count();
    let start = old.first().map_or(0, |c| c.start_offset);
    let end = old.last().map_or(0, |c| c.end_offset);
    let mut body = Vec::new();
    for chunk in &old {
        let len = usize::try_from(
            chunk
                .end_offset
                .checked_sub(chunk.start_offset)
                .context("cold chunk ends before it starts")?,
        )?;
        body.extend_from_slice(
            &cold
                .read_chunk_range(chunk, chunk.start_offset, len)
                .await
                .context("read slice")?,
        );
    }
    let path = ursula_runtime::new_cold_chunk_path_in_generation(&a, generation, start, end);
    let size = cold
        .write_chunk(&path, &body)
        .await
        .context("write replacement")?;
    let result = rt
        .compact_cold(CompactColdRequest {
            stream_id: a.clone(),
            old_chunks: old,
            replacement: ColdChunkRef {
                start_offset: start,
                end_offset: end,
                s3_path: path,
                object_size: size,
                object_offset: 0,
                shared_object: false,
                payload_digest: blake3::hash(&body).to_hex().to_string(),
            },
            gc_not_before_ms: 0,
        })
        .await;
    let (after, _) = snap(&rt).await?;
    let refs_after = after
        .stream_snapshot
        .streams
        .iter()
        .find(|e| e.metadata.stream_id == a)
        .map(|e| e.cold_chunks.len());
    let ok = result.is_ok();
    shutdown(&rt, wal).await?;
    Ok((
        json!({
            "engine": engine.label(),
            "flushed_slices": flushed,
            "stream_a_shared_refs_before": shared,
            "compact_cold_result": match &result { Ok(r) => format!("OK {r:?}"), Err(e) => format!("ERR {e}") },
            "stream_a_refs_after": refs_after,
            "pending_cold_gc_after": after.stream_snapshot.pending_cold_gc.len(),
        }),
        ok,
    ))
}

async fn compact(sink: &mut Sink) -> Result<Outcome> {
    let mut outcome = Outcome::default();
    for engine in [Engine::Memory, Engine::Raft] {
        let (row, ok) = compact_on(engine).await?;
        sink.row(&row)?;
        outcome.check(
            &format!("f2_shared_compact_cold_{}", engine.label()),
            "CompactCold of shared pack slices succeeds (F2 Raft branch); value 1 = refused",
            if ok { 0.0 } else { 1.0 },
            0.0,
        );
    }
    Ok(outcome)
}
