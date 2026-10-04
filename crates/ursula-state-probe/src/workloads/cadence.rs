//! Snapshot cadence (bounded-stream-state F12e, §7.3 from B6): drives
//! `--groups` real `StreamStateMachine`s with uniform round-robin JSON
//! appends and the flush passes the cold worker would run, counts the Raft
//! log bytes every applied command adds (`StreamCommand::log_bytes_estimate`,
//! what the Raft state machine's log gauge counts), and snapshots groups
//! exactly when the production policy (`ursula_raft::snapshot_cadence`)
//! says so, sizing each snapshot with the production codec.
//!
//! It reports snapshot bytes written per appended log byte and the largest
//! node log (log applied since each group's last snapshot, summed over the
//! groups) seen at a driver tick, against the node log budget. W1 is
//! `--groups=1 --node-groups=128` (one heavy stream on a default node); the
//! uniform workload is `--groups=128 --streams-per-group=4`.

use anyhow::Result;
use clap::Args;
use serde_json::json;
use ursula_raft::snapshot_cadence::GroupLogGauge;
use ursula_raft::snapshot_cadence::SnapshotCadence;
use ursula_stream::StreamCommand;
use ursula_stream::StreamStateMachine;

use crate::codec;
use crate::out::Outcome;
use crate::out::Sink;
use crate::out::round3;
use crate::payload;
use crate::smx;

/// The F12e target: snapshot bytes per appended log byte (§8 B6 exit).
pub const MAX_SNAPSHOT_BYTES_PER_LOG_BYTE: f64 = 0.6;

#[derive(Debug, Clone, Args)]
pub struct CadenceArgs {
    /// Raft groups simulated on the node.
    #[arg(long, default_value_t = 1)]
    pub groups: usize,
    /// Group count the policy's floor assumes (defaults to `--groups`).
    #[arg(long)]
    pub node_groups: Option<usize>,
    #[arg(long, default_value_t = 1)]
    pub streams_per_group: usize,
    /// Appends across the node, round-robin over groups, then streams.
    #[arg(long, default_value_t = 300_000)]
    pub appends: u64,
    #[arg(long, default_value_t = 200)]
    pub rec_bytes: usize,
    /// Node log-byte budget in MiB (1 GiB by default).
    #[arg(long, default_value_t = 1024)]
    pub budget_mib: u64,
    /// Group hot size (real bytes) at which the group flushes, in KiB.
    #[arg(long, default_value_t = 8192)]
    pub flush_kib: usize,
    /// Appends between driver ticks.
    #[arg(long, default_value_t = 1_000)]
    pub tick_appends: u64,
    /// Groups one driver tick may snapshot.
    #[arg(long, default_value_t = 16)]
    pub max_groups_per_tick: usize,
    #[arg(long)]
    pub name: Option<String>,
}

pub fn default_name(args: &CadenceArgs) -> String {
    args.name
        .clone()
        .unwrap_or_else(|| format!("cadence_g{}", args.groups))
}

struct Group {
    machine: StreamStateMachine,
    gauge: GroupLogGauge,
    streams: Vec<ursula_shard::BucketStreamId>,
    commit_index: u64,
    packs: smx::PackPaths,
    flush: smx::FlushStats,
}

impl Group {
    fn apply(&mut self, command: StreamCommand, what: &str) -> Result<()> {
        self.gauge.record_applied(command.log_bytes_estimate());
        self.commit_index += 1;
        smx::ok(self.machine.apply(command), what)?;
        Ok(())
    }
}

fn new_group(index: usize, streams_per_group: usize) -> Result<Group> {
    let mut group = Group {
        machine: StreamStateMachine::new(),
        gauge: GroupLogGauge::default(),
        streams: Vec::new(),
        commit_index: 0,
        packs: smx::PackPaths::default(),
        flush: smx::FlushStats::default(),
    };
    group.apply(
        StreamCommand::CreateBucket {
            bucket_id: "bkt1".to_owned(),
        },
        "create bucket",
    )?;
    for stream in 0..streams_per_group {
        let id = smx::sid("bkt1", &format!("g{index:04}"), &format!("s{stream:03}"));
        group.apply(
            StreamCommand::CreateStream {
                stream_id: id.clone(),
                content_type: smx::JSON.to_owned(),
                initial_payload: bytes::Bytes::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                now_ms: smx::T0,
            },
            "create stream",
        )?;
        group.streams.push(id);
    }
    Ok(group)
}

/// Flushes the group once its hot size reaches `flush_bytes`, the way
/// the cold worker's group drain would, counting each `FlushCold` as log.
fn maybe_flush(group: &mut Group, flush_bytes: usize) -> Result<()> {
    if group.machine.total_hot_payload_bytes() < flush_bytes as u64 {
        return Ok(());
    }
    let published = smx::flush_pass(
        &mut group.machine,
        1,
        flush_bytes,
        &mut group.packs,
        &mut group.flush,
    )?;
    for (stream_id, chunk) in published {
        group.gauge.record_applied(
            StreamCommand::FlushCold {
                cold_generation: group
                    .machine
                    .cold_index_generation(&stream_id)
                    .unwrap_or_default(),
                stream_id,
                chunk,
            }
            .log_bytes_estimate(),
        );
        group.commit_index += 1;
    }
    Ok(())
}

fn snapshot_bytes(group: &Group) -> Result<u64> {
    let snapshot = codec::group_snapshot(group.machine.snapshot(), Vec::new(), group.commit_index);
    Ok(codec::encode_bytes(snapshot)?.len() as u64)
}

pub fn run(args: &CadenceArgs, sink: &mut Sink) -> Result<Outcome> {
    let name = default_name(args);
    let groups_n = args.groups.max(1);
    let node_groups = args.node_groups.unwrap_or(groups_n).max(1);
    let budget = args.budget_mib.saturating_mul(1 << 20);
    let cadence = SnapshotCadence::new(
        budget,
        node_groups,
        ursula_raft::snapshot_cadence::DEFAULT_SNAPSHOT_BACKSTOP_ENTRIES,
    );
    let flush_bytes = args.flush_kib.saturating_mul(1024);
    sink.row(
        &json!({"workload": name, "groups": groups_n, "node_groups": node_groups,
        "streams_per_group": args.streams_per_group, "appends": args.appends,
        "rec_bytes": args.rec_bytes, "budget_bytes": budget, "floor_bytes": cadence.floor_bytes,
        "flush_bytes": flush_bytes}),
    )?;
    let mut groups = (0..groups_n)
        .map(|index| new_group(index, args.streams_per_group.max(1)))
        .collect::<Result<Vec<_>>>()?;
    let mut rng = payload::Rng::new(11);
    let mut snapshots = 0u64;
    let mut snapshot_total = 0u64;
    let mut pressure_ticks = 0u64;
    let mut max_node_log = 0u64;
    let mut largest_snapshot = 0u64;
    let tick = args.tick_appends.max(1);
    for n in 0..args.appends {
        let group_index = usize::try_from(n % groups_n as u64).unwrap_or(0);
        let round = n / groups_n as u64;
        let Some(group) = groups.get_mut(group_index) else {
            continue;
        };
        let stream_index = usize::try_from(round % group.streams.len() as u64).unwrap_or(0);
        let Some(stream_id) = group.streams.get(stream_index).cloned() else {
            continue;
        };
        let record = payload::json_record(&mut rng, n, args.rec_bytes);
        group.apply(
            StreamCommand::Append {
                stream_id,
                content_type: Some(smx::JSON.to_owned()),
                payload: bytes::Bytes::from(record),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: smx::T0 + n,
            },
            "append",
        )?;
        maybe_flush(group, flush_bytes)?;
        if (n + 1) % tick == 0 || n + 1 == args.appends {
            let progress = groups
                .iter()
                .map(|group| group.gauge.progress())
                .collect::<Vec<_>>();
            let plan = cadence.plan(&progress, args.max_groups_per_tick);
            max_node_log = max_node_log.max(plan.node_log_bytes);
            if plan.pressure {
                pressure_ticks += 1;
            }
            for index in plan.groups {
                let Some(group) = groups.get_mut(index) else {
                    continue;
                };
                let mark = group.gauge.mark();
                let bytes = snapshot_bytes(group)?;
                group.gauge.record_snapshot(mark, bytes);
                snapshots += 1;
                snapshot_total += bytes;
                largest_snapshot = largest_snapshot.max(bytes);
            }
        }
    }
    // Log appended in total: what was snapshotted plus what is still held.
    let log_total = groups
        .iter()
        .map(|group| group.gauge.mark().bytes())
        .sum::<u64>();
    let ratio = if log_total == 0 {
        0.0
    } else {
        snapshot_total as f64 / log_total as f64
    };
    let flushes: u64 = groups.iter().map(|group| group.flush.passes).sum();
    sink.row(
        &json!({"workload": name, "log_bytes": log_total, "snapshot_bytes": snapshot_total,
        "snapshots": snapshots, "snapshot_bytes_per_log_byte": round3(ratio),
        "max_node_log_bytes": max_node_log, "pressure_ticks": pressure_ticks,
        "largest_snapshot_bytes": largest_snapshot, "flush_passes": flushes}),
    )?;
    let mut outcome = Outcome::default();
    outcome.metric_u64("log_bytes", log_total);
    outcome.metric_u64("snapshot_bytes", snapshot_total);
    outcome.metric_u64("snapshots", snapshots);
    outcome.metric_u64("max_node_log_bytes", max_node_log);
    outcome.metric_u64("pressure_ticks", pressure_ticks);
    outcome.metric("snapshot_bytes_per_log_byte", round3(ratio));
    outcome.check(
        "f12_snapshot_bytes_per_log_byte",
        "snapshot bytes written per appended log byte <= 0.6 (F12e)",
        ratio,
        MAX_SNAPSHOT_BYTES_PER_LOG_BYTE,
    );
    outcome.check(
        "f12_node_log_within_budget",
        "log held since each group's last snapshot, summed over the node, <= the node log budget (F12e)",
        max_node_log as f64,
        budget as f64,
    );
    Ok(outcome)
}
