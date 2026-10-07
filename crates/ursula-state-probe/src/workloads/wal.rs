//! Raft WAL memory: drives `--groups` real log stores on one core journal
//! (`RaftWal` in a temporary directory, fsync `never`)
//! with round-robin appends of `--entry-bytes` payloads. Each group is purged
//! the way the snapshot driver's snapshots purge it: once it holds
//! `--retain-kib` of log, up to its last entry minus `--keep` (OpenRaft's
//! `max_in_snapshot_log_to_keep`). A group that `--quiet-every` skips appends
//! only once per that many rounds, so its few entries stay behind in old
//! journal bytes.
//!
//! It samples the live heap (counting allocator) every `--sample-every`
//! appends and reports the heap the WAL holds at the end of the run and its
//! peak, against the log the groups retain. With the retained log far above
//! the entry caches (`--cache-kib` per group), the heap must stay near the
//! caches plus the index: memory per group is O(index + cache), not
//! O(retained entries).

use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use clap::Args;
use futures_util::future::try_join_all;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::entry::RaftEntry;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftLogStorageExt;
use openraft::vote::RaftLeaderId;
use openraft::vote::leader_id_adv::CommittedLeaderId;
use serde_json::json;
use ursula_config::WalFsync;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_raft::wal::JournalTuning;
use ursula_raft::wal::RaftGroupFileLogStore;
use ursula_raft::wal::RaftWal;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::RuntimeMetrics;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;

use crate::alloc;

/// The heap the WAL may hold beyond its entry caches: the index (a few
/// dozen bytes per retained entry), the writer's write buffer and the
/// buffers of a read or a rewrite.
const INDEX_AND_BUFFER_BYTES: u64 = 16 << 20;
use crate::out::Outcome;
use crate::out::Sink;

#[derive(Debug, Clone, Args)]
pub struct WalArgs {
    /// Raft groups on the core.
    #[arg(long, default_value_t = 16)]
    pub groups: u32,
    /// Payload bytes per entry.
    #[arg(long, default_value_t = 7168)]
    pub entry_bytes: usize,
    /// Appends across the core, one per group per round.
    #[arg(long, default_value_t = 100_000)]
    pub appends: u64,
    /// Log a group retains before a purge, in KiB.
    #[arg(long, default_value_t = 8192)]
    pub retain_kib: u64,
    /// Entries a purge keeps.
    #[arg(long, default_value_t = 64)]
    pub keep: u64,
    /// Every this many groups, one is quiet: it appends once per
    /// `--quiet-rounds` rounds. 0 disables quiet groups.
    #[arg(long, default_value_t = 0)]
    pub quiet_every: u32,
    #[arg(long, default_value_t = 256)]
    pub quiet_rounds: u64,
    /// Recent entries each group caches, in KiB.
    #[arg(long, default_value_t = 4096)]
    pub cache_kib: u64,
    /// Journal segment size, in KiB.
    #[arg(long, default_value_t = 65536)]
    pub segment_kib: u64,
    /// Appends between heap samples.
    #[arg(long, default_value_t = 1_000)]
    pub sample_every: u64,
    #[arg(long)]
    pub name: Option<String>,
}

pub fn default_name(args: &WalArgs) -> String {
    args.name
        .clone()
        .unwrap_or_else(|| format!("wal_g{}", args.groups))
}

fn placement(group_id: u32) -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(group_id),
        raft_group_id: RaftGroupId(group_id),
    }
}

fn log_id(index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
    LogId {
        leader_id: CommittedLeaderId::new(1, 1),
        index,
    }
}

fn entry(index: u64, group_id: u32, entry_bytes: usize) -> EntryOf<UrsulaRaftTypeConfig> {
    EntryOf::<UrsulaRaftTypeConfig>::new(
        log_id(index),
        EntryPayload::Normal(GroupWriteCommand::Stream(StreamCommand::Append {
            stream_id: BucketStreamId::new("wal-probe", format!("stream-{group_id}")),
            content_type: Some("application/octet-stream".to_owned()),
            // A fresh buffer per entry, as an entry decoded from a request owns one.
            payload: Bytes::from(vec![u8::try_from(index % 251).unwrap_or(0); entry_bytes]),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        })),
    )
}

/// One group's store and how much of its log it holds.
struct Group {
    id: u32,
    store: Arc<RaftGroupFileLogStore>,
    last: u64,
    purged: u64,
    quiet: bool,
}

impl Group {
    fn retained_bytes(&self, entry_bytes: usize) -> u64 {
        self.last
            .saturating_sub(self.purged)
            .saturating_mul(u64::try_from(entry_bytes).unwrap_or(u64::MAX))
    }
}

/// Appends one entry to `group` and purges it once it holds `retain` bytes.
async fn step(group: &mut Group, entry_bytes: usize, retain: u64, keep: u64) -> Result<()> {
    let index = group.last.saturating_add(1);
    group
        .store
        .blocking_append([entry(index, group.id, entry_bytes)])
        .await
        .with_context(|| format!("append group {} entry {index}", group.id))?;
    group.last = index;
    if group.retained_bytes(entry_bytes) >= retain {
        let upto = group.last.saturating_sub(keep);
        if upto > group.purged {
            group
                .store
                .purge(log_id(upto))
                .await
                .with_context(|| format!("purge group {} to {upto}", group.id))?;
            group.purged = upto;
        }
    }
    Ok(())
}

pub fn run(args: &WalArgs, sink: &mut Sink) -> Result<Outcome> {
    let name = default_name(args);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("build runtime")?;
    runtime.block_on(run_async(args, &name, sink))
}

async fn run_async(args: &WalArgs, name: &str, sink: &mut Sink) -> Result<Outcome> {
    let groups_n = args.groups.max(1);
    let retain = args.retain_kib.saturating_mul(1024);
    let cache = args.cache_kib.saturating_mul(1024);
    sink.row(
        &json!({"workload": name, "groups": groups_n, "entry_bytes": args.entry_bytes,
        "appends": args.appends, "retain_bytes": retain, "keep": args.keep,
        "cache_bytes": cache, "segment_bytes": args.segment_kib.saturating_mul(1024),
        "quiet_every": args.quiet_every, "quiet_rounds": args.quiet_rounds}),
    )?;
    let root = tempfile::tempdir().context("WAL root")?;
    let metrics = RuntimeMetrics::new(1, usize::try_from(groups_n).unwrap_or(usize::MAX));
    let baseline = alloc::heap().bytes;
    let mut groups = {
        let factory = RaftWal::start_with(
            root.path(),
            JournalTuning {
                fsync: WalFsync::Never,
                segment_bytes: args.segment_kib.saturating_mul(1024),
                group_cache_bytes: args.cache_kib.saturating_mul(1024),
            },
            &ursula_shard::StaticShardMap::new(1, usize::try_from(groups_n)?)?,
        )
        .context("start the WAL")?;
        (0..groups_n)
            .map(|id| {
                Ok(Group {
                    id,
                    store: factory
                        .open(placement(id), metrics.group_engine_metrics())
                        .with_context(|| format!("open group {id}"))?,
                    last: 0,
                    purged: 0,
                    quiet: args.quiet_every != 0 && id.checked_rem(args.quiet_every) == Some(0),
                })
            })
            .collect::<Result<Vec<_>>>()?
    };
    let opened = alloc::heap().bytes;
    alloc::reset_peak();
    let rounds = args.appends.div_ceil(u64::from(groups_n));
    let sample_rounds = args.sample_every.div_ceil(u64::from(groups_n)).max(1);
    let mut samples = Vec::new();
    for round in 0..rounds {
        let quiet_round = round.checked_rem(args.quiet_rounds.max(1)) == Some(0);
        try_join_all(
            groups
                .iter_mut()
                .filter(|group| !group.quiet || quiet_round)
                .map(|group| step(group, args.entry_bytes, retain, args.keep)),
        )
        .await?;
        if round.checked_rem(sample_rounds) == Some(0) {
            samples.push(alloc::heap().bytes.saturating_sub(opened));
        }
    }
    let heap = alloc::heap().bytes.saturating_sub(opened);
    let peak = alloc::peak_bytes().saturating_sub(opened);
    let retained: u64 = groups
        .iter()
        .map(|group| group.retained_bytes(args.entry_bytes))
        .sum();
    let second_half = samples.get(samples.len() / 2..).unwrap_or_default();
    let steady = if second_half.is_empty() {
        heap
    } else {
        second_half
            .iter()
            .fold(0_i64, |total, sample| total.saturating_add(*sample))
            .checked_div(i64::try_from(second_half.len()).unwrap_or(1))
            .unwrap_or(heap)
    };
    let wal = metrics.snapshot();
    let heap_u64 = u64::try_from(heap.max(0)).unwrap_or(0);
    let peak_u64 = u64::try_from(peak.max(0)).unwrap_or(0);
    let steady_u64 = u64::try_from(steady.max(0)).unwrap_or(0);
    sink.row(
        &json!({"workload": name, "stores_heap_bytes": opened.saturating_sub(baseline),
        "heap_bytes": heap_u64, "steady_heap_bytes": steady_u64, "peak_heap_bytes": peak_u64,
        "retained_bytes": retained,
        "heap_per_group_bytes": heap_u64.checked_div(u64::from(groups_n)).unwrap_or(0),
        "wal_physical_bytes": wal.wal_physical_bytes, "wal_segments": wal.wal_segments,
        "wal_reclaims": wal.wal_reclaims, "wal_reclaimed_bytes": wal.wal_reclaimed_bytes,
        "wal_rewritten_bytes": wal.wal_rewritten_bytes, "wal_cache_bytes": wal.wal_cache_bytes,
        "wal_indexed_entries": wal.wal_indexed_entries}),
    )?;
    drop(groups.drain(..));
    // The caches at most, plus the index and the writer's buffers.
    let bound = cache
        .saturating_mul(u64::from(groups_n))
        .saturating_add(INDEX_AND_BUFFER_BYTES);
    let mut outcome = Outcome::default();
    outcome.metric_u64("heap_bytes", heap_u64);
    outcome.metric_u64("peak_heap_bytes", peak_u64);
    outcome.metric_u64("retained_bytes", retained);
    outcome.check(
        "wal_heap_within_index_and_caches",
        "WAL heap after the run <= the groups' entry caches plus 16 MiB of index and buffers",
        heap_u64 as f64,
        bound as f64,
    );
    outcome.check(
        "wal_peak_heap_within_index_and_caches",
        "WAL peak heap <= the groups' entry caches plus 16 MiB of index and buffers",
        peak_u64 as f64,
        bound as f64,
    );
    Ok(outcome)
}
