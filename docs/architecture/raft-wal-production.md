# Production Raft WAL with an in-memory state machine

Status: accepted for the first production-hardening increment. The operational
rollout gate remains tracked by [#273](https://github.com/tonbo-io/ursula/issues/273).

## Decision

Ursula keeps its deterministic in-memory stream state machine and its existing
per-core, cross-group WAL writer. It adopts the useful properties demonstrated
by OpenRaft's `raft-kv-log-wal-sm-mem` and `log-wal` examples without adopting
one independent WAL worker and cache per Raft group.

The recovery model is:

```text
persisted group snapshot + committed Raft WAL suffix -> in-memory state machine
hot stream tail + immutable S3 chunks                -> Durable Stream history
```

These are two different logs. S3 compaction makes old application payload
available outside the hot ring; it does not by itself authorize deletion of a
Raft entry. Physical WAL reclaim is safe only after OpenRaft has persisted a
state-machine snapshot and advanced the group's purge boundary.

## Why the upstream example is useful but not the production topology

The upstream example validates the same high-level split Ursula wants: a
durable log can rebuild a volatile application state machine after restart. Its
`raft-log` adapter also demonstrates checksummed chunks, an indexed cache,
exclusive ownership, truncation, purge, and asynchronous flush completion.

Ursula has a different multiplicity. A node owns many independent Raft groups
and deliberately batches their durable writes on one writer per core. Giving
every group its own worker, file-descriptor set, and cache would multiply fixed
costs by the group count. In particular, `raft-log 0.4.5` defaults to a 1 GiB
payload cache per store, which is unsuitable as an implicit per-group budget.

The benchmark-only adapter therefore caps each `raft-log` cache at 4 MiB and
4,096 entries and waits for its durable flush callback. It is a comparison
backend, not a production dependency.

## Measurement

Run the repeatable comparison with:

```bash
URSULA_WAL_BENCH_RUNS=3 \
  URSULA_WAL_BENCH_FILTER='groups=16/payload=256' \
  scripts/bench_raft_wal.sh target/raft-wal-bench
```

One directional run on an Apple Silicon development host, using the same
filesystem and toolchain for all backends, produced:

| Backend | Three-run mean for 16 durable appends | Stddev | CV |
|---|---:|---:|---:|
| Ursula direct file per group | 53.531 ms | 1.565 ms | 2.92% |
| Ursula shared file per core | 18.952 ms | 3.883 ms | 20.49% |
| Upstream `raft-log` per group | 88.058 ms | 3.466 ms | 3.94% |

The shared result has visible host noise, but its slowest individual run still
beats the alternatives by more than the five-percent decision threshold. The
result supports retaining cross-group fsync batching; it is not a general claim
that the current file format is faster than every segmented WAL workload.

`disk_wal` also covers one and many groups, 256-byte through 64-KiB payloads,
append plus committed-marker persistence, recent reads, and restart replay.
Set `URSULA_WAL_BENCH_FULL=1` for the wider matrix. A performance-sensitive
follow-up must publish at least three independent runs from the same host and
filesystem and explain any throughput or latency regression over five percent.

## On-disk contract

Each active core owns one `core-N/journal.bin`. The v1 format starts with a
magic/version header and encodes every record as:

```text
u32 payload length | u32 CRC32 | MessagePack payload
```

Recovery refuses unknown versions, impossible lengths, checksum failures, and
decoding failures. An incomplete final frame is treated as a crash-torn tail
and truncated to the last complete checksummed frame. The decoder bounds a
single frame at 512 MiB before allocating.

The writer holds an exclusive advisory lock in `journal.bin.lock`. A second
process receives a diagnostic error naming the journal, lock path, and recorded
owner PID instead of concurrently modifying the same WAL.

An existing unversioned journal is migrated under that lock. Migration streams
records into a checksummed temporary file, syncs it, preserves the original as
`journal.bin.v0.bak`, atomically installs v1, and syncs the parent directory.
The backup is intentionally retained for rollback and should be removed only
after the staged upgrade has been validated.

## Online reclaim and the single-file question

The previous journal was logically purged but physically append-only for the
lifetime of a process. It compacted only during restart, so a long-running node
could retain every historical WAL frame in one growing file even while S3 and
the in-memory state had already converged.

The v1 writer performs an online generation checkpoint after a purge or
truncate once the core journal reaches 64 MiB:

1. append and sync the purge/truncate record;
2. replay the durable journal into the current live state of every group;
3. write and sync a new checksummed generation containing only that state;
4. atomically replace `journal.bin` and sync its parent directory;
5. reopen the append handle and continue batching.

This is intentionally a single active generation rather than a directory of
per-group segments. It gives the property the Epic needs—obsolete physical
bytes are reclaimed online and restart scan work converges toward live retained
state plus at most the 64-MiB trigger slack—without losing per-core batching or
introducing hundreds of independent segment managers. Quiet groups interleaved
with busy groups are included in the generation checkpoint, so they do not pin
historical bytes forever.

If a crash occurs before the rename, the old synced generation remains valid.
If it occurs after the rename, the new generation is already synced. The
parent-directory sync makes the replacement durable. Reclaim never precedes the
OpenRaft purge record that establishes the safe logical boundary.

## Durability and application boundary

Acknowledged application writes retain the existing quorum contract. Append
completion is tied to the durable WAL flush callback. Vote, committed-marker,
truncate, and purge records remain synchronously persisted.

The committed marker is intentionally not relaxed in this increment. A fresh
state machine restores the latest persisted snapshot and re-applies only entries
through that marker. A durable uncommitted suffix remains available for Raft
replication or truncation but is never exposed as application state after
restart. OpenRaft's log-store conformance suite and an explicit committed-tail
restart test cover this boundary.

The apparent extra committed-marker sync is partially amortized by the per-core
writer. Changing it requires crash testing that proves no acknowledged-state or
replay regression and benchmark evidence beyond normal variance.

## Metrics and operational interpretation

The runtime metrics snapshot now exposes bounded-cardinality core/group WAL
counters:

- `wal_fsyncs` and `wal_fsync_records` show actual physical flush count and the
  number of logical records sharing those flushes;
- `wal_reclaims`, `wal_reclaimed_bytes`, and `wal_reclaim_ns` show online
  checkpoint frequency, effect, and cost;
- `wal_physical_bytes` reports the current active journal size, summed across
  cores globally;
- the existing `wal_batches`, `wal_records`, `wal_write_ns`, and `wal_sync_ns`
  continue to report logical store activity and latency.

`wal_fsync_records / wal_fsyncs` is the effective physical batch size. During a
steady workload with snapshots and purge, `wal_physical_bytes` should form a
sawtooth bounded by live retained state and checkpoint slack. A monotonically
growing value together with zero `wal_reclaims` means the snapshot/purge driver
or its safety prerequisite is stalled; it is not evidence that S3 compaction
alone should delete the WAL.

Disk-free-space admission, readiness reporting, restart scan metrics, and the
multi-node rolling-restart soak are the final operational gate in #273. Helm's
production-facing default remains `raft.storageMode=logDir` with a per-pod PVC;
`memory` remains an explicit development, benchmark, and chaos mode.

## Verification envelope

The storage increment is covered by:

- OpenRaft's complete log-store conformance suite against
  `RaftGroupFileLogStore`;
- checksum corruption, unknown format, oversized frame, and torn-tail tests;
- exclusive-owner and legacy migration/rollback tests;
- snapshot/purge/truncate recovery tests across the shared core journal;
- a restart test proving only the committed suffix rebuilds the state machine;
- an online checkpoint test proving the replacement writer accepts and replays
  subsequent appends;
- the existing late-learner, S3 cold-manifest, and durable multi-node tests.

The format change does not require an OpenRaft upgrade. Ursula remains pinned to
its current patched OpenRaft release so storage-format risk and consensus-library
risk are evaluated independently.
