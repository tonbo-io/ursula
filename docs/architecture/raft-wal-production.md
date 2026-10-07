# Production Raft WAL with an in-memory state machine

Status: accepted, including the operational rollout gate tracked by
[#273](https://github.com/tonbo-io/ursula/issues/273).

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
| Ursula direct file per group | 55.702 ms | 3.079 ms | 5.53% |
| Ursula shared file per core | 19.475 ms | 1.050 ms | 5.39% |
| Upstream `raft-log` per group | 91.762 ms | 3.931 ms | 4.28% |

The shared writer is about 2.9 times faster than the direct-per-group writer and
4.7 times faster than the upstream per-group adapter in this profile. A main
worktree carrying only the same benchmark harness measured the old shared WAL
at 20.345 ms with a 1.532-ms standard deviation. The new checksummed path's
4.3-percent lower mean is inside the observed variance, so the defensible
before/after conclusion is no measurable regression, not a claimed speedup.
The result supports retaining cross-group fsync batching; it is not a general
claim that the current file format is faster than every segmented WAL workload.

`disk_wal` also covers one and many groups, 256-byte through 64-KiB payloads,
append plus committed-marker persistence, recent reads, and restart replay.
Set `URSULA_WAL_BENCH_FULL=1` for the wider matrix. A performance-sensitive
follow-up must publish at least three independent runs from the same host and
filesystem and explain any throughput or latency regression over five percent.

The fsync-policy profile uses:

```bash
URSULA_WAL_BENCH_RUNS=3 \
  URSULA_WAL_BENCH_GROUP=disk_wal_append_and_commit \
  URSULA_WAL_BENCH_FILTER='disk_wal_append_and_commit.*groups=16' \
  scripts/bench_raft_wal.sh target/raft-wal-append-commit
```

Deferring the replay-hint fsync reduced the shared writer's three-run mean from
35.240 ms (1.758-ms standard deviation) to 17.336 ms (0.477-ms standard
deviation), a 50.8-percent improvement. The direct-per-group control improved
from 83.453 ms to 57.054 ms. This is the measured benefit of removing the
second fsync; it does not change the durable append acknowledgement.

## On-disk contract

Each active core owns one `core-N/journal.bin`. Format epoch 3 starts the
file with a 32-byte header (magic, version, header length, a 64-bit generation
sequence and a CRC32 of the header) and encodes every record as:

```text
u32 payload length | u32 header CRC | u32 payload CRC | MessagePack payload
```

The header CRC covers the generation sequence and the length, so a damaged
length is detected before replay trusts it. The payload CRC covers the
sequence, the length and the payload. A new journal is generation 1, and every
rewrite (startup compaction, online reclaim) writes the next generation, so a
frame verifies only in the file generation that wrote it.

Replay has two modes:

- `Strict` tolerates only an incomplete final frame, which it truncates. Any
  other frame that fails verification fails recovery with its frame number and
  offset.
- `VerifiedPrefix` keeps the frames before the first one that fails
  verification and truncates the rest, reporting the dropped bytes.

Startup recovery picks the mode from the run state (below). Online reclaim
uses `Strict` and never truncates: a live journal that fails verification
stops the writer instead. A journal read as a verified prefix is always
rewritten as its next generation, so every frame it keeps is on disk before
the core writes again, even a frame whose `fsync` failed and that only the
page cache still held.

The writer is fail-stop. A failed write or `fsync` of the journal, or of the
directory that publishes a new generation, poisons the core's writer for good:
the failing batch and every later request fail with `WriterPoisoned`, and the
writer never appends after a possibly partial frame or retries an `fsync`
whose dirty pages the kernel may already have dropped. The process then
aborts, because only a restart can re-read what is really on disk. Deterministic
simulation keeps the poisoned writer instead, so it can observe it and restart
the node. A reclaim that fails before it replaces the journal leaves the
journal as it was: the batch that triggered it stays acknowledged, the failure
is logged and counted in `wal_reclaim_failures`, and appends continue.
Both modes refuse unknown versions, a damaged file header, oversized frames
and frames whose checksums verify but whose payload does not decode. The
decoder bounds a single frame at 512 MiB before allocating.

The writer holds an exclusive advisory lock in `journal.bin.lock`. A second
process receives a diagnostic error naming the journal, lock path, and recorded
owner PID instead of concurrently modifying the same WAL. The node also holds
`wal.lock` at the WAL root for the whole run, so a second process never
touches the run state.

### Metadata and run-state files

Votes are not journal records. Each core keeps `core-N/journal.meta` next to
its journal with every group's vote and log state:

- `empty`: the replica never persisted an entry or a purge of the group.
- `initialized`: its log holds every entry it acknowledged.
- `recovering`: its log may be missing entries it acknowledged. The group's
  recovery gate stays closed (below) until the state returns to
  `initialized`.

A group leaves `empty` in the batch that persists its first entry or purge,
before either is acknowledged, and never returns to it. The first entry
records `initialized`, unless the group's gate is closed while the replica
holds nothing of it (its history is unknown, as for a wiped disk), in which
case it records `recovering`. Recovery reads votes and log states from this
file. A group whose journal holds entries but whose metadata says `empty` (a
crash between the two writes) is repaired, as `recovering` when the node is
recovering. A group that is `initialized` but whose journal holds nothing of
it lost its log, and becomes `recovering`. The file also records the recovery
epoch in which the journal was last read in full.

The node keeps `run-state.bin` at the WAL root: the boot id of the run that
last opened the journals (`/proc/sys/kernel/random/boot_id`, absent on other
platforms), that run's fsync policy, how it ended (`running`, `clean` or
`poisoned`) and a recovery epoch counter.

Both files are replaced whole: a temporary file is written, `fsync`ed,
renamed over the old file and published with a directory `fsync`, so a crash
leaves either version. Each file carries its own magic, the format epoch, a
length and a CRC32, and a damaged file fails startup.

At startup the node reads the run state, decides how to read the journals,
and durably records itself as `running` with the current boot id before any
journal write:

| Previous run | Replay | Recovery state |
| --- | --- | --- |
| No run state, no core journal holds a record | `Strict` | normal |
| No run state, a core journal holds records | `VerifiedPrefix` | recovering (unknown history) |
| `clean` | `Strict` | normal |
| `running`, same boot id (process crash) | `Strict` | normal |
| `running`, other or unknown boot id (host crash), policy `always` | `VerifiedPrefix` | normal |
| `running`, other or unknown boot id (host crash), policy `never` | `VerifiedPrefix` | recovering |
| `poisoned` | `VerifiedPrefix` | recovering |

A host crash needs the verified prefix even under `always`, because committed
and truncate markers are written without `fsync` and writeback can leave a
hole before acknowledged frames. A run that starts after a host crash or a
poisoned run begins a new recovery epoch. Cores open lazily, so a core whose
metadata shows an older epoch has not been read since the crash and is still
read as a verified prefix, however the runs in between ended.

"Recovering" means the node's logs may be missing entries it acknowledged.
The node logs it at warn, reports it in the metrics JSON (`wal_recovery`), as
the `ursula.wal.recovering` gauge, and through
`RaftGroupHandleRegistry::wal_recovery_state`. Before it records itself as
`running`, a recovering run moves every `initialized` group of every core into
`recovering`, so a crash at any later point, a process crash or a clean
shutdown before the gates open, comes back gated: the per-group state, not the
node's run state, carries the recovery forward.

### Recovery gate

`rejoin` holds one gate per group and replica, for both log stores. A
disk-WAL gate starts from the group's log state: open when `initialized`,
closed when `recovering`, and closed with an unknown history when `empty`. A
memory-WAL gate always starts closed with an unknown history.

While closed, the replica does not campaign (its Raft core starts with
elections disabled), refuses a leadership transfer to itself, and refuses
every vote once it knows the group holds entries: its log state says so, a
leader reported a commit index of 1 or more, or a candidate's log reached
index 1. An unknown history passes candidates whose log is only the membership
entry, so a new group's first election still works. The replica accepts
appends from any leader whose vote is not lower than its own, which the
metadata file restored before the Raft core started; an older-term leader
gets `HigherVote`.

OpenRaft restores a node whose persisted vote is a committed vote for itself
as the leader of that term without an election. A recovering replica that led
the group may have lost entries it appended while its followers kept them, so
restored leadership would append new entries under their log ids and fork the
log. A recovering replica's own committed vote is therefore presented
uncommitted: it starts as a follower that already voted in that term. Under
`always` the store hands OpenRaft only entries whose batch was `fsync`ed, so a
replica that is not recovering keeps every entry it ever replicated.

The barrier driver asks the current leader for a fresh outbound ReadIndex
barrier (`RejoinBarrier`) and opens the gate once the replica applied the
barrier's committed index. Inbound replication alone never opens it. A
disk-WAL gate first records `initialized`, then opens, and the driver
refreshes the group's election policy after the gate opened (refreshing
before the final check could leave elections disabled).

On the leader, a follower that answers `Conflict` at or below the index it
had matched in this leadership lost entries. The network layer hands OpenRaft
an error instead, and the heal driver removes the voter, adds it back as a
learner and promotes it once it caught up. Desired voters come from the static
configuration. When the followers that lost entries are a majority, no removal
can commit; the leader holds every committed entry, so it allows OpenRaft one
rewind per follower and replicates its log to them again. OpenRaft does not
survive a rewind driven by a heartbeat's conflict while a replication stream's
acknowledgements are in flight, so only the conflict of a request that carried
entries reaches it, and an idle group gets an unchanged membership entry to
replicate. The allowance ends once replication progress shows the old matched
index reset or a later success confirms it.

A gated replica that gets no barrier and applies nothing for 30 s reports its
group stalled: a majority of the voters may be gated, so the group has no
leader and refuses writes. Readiness answers `recovery_stalled`, and
`recovery_gates` in the metrics JSON and the `ursula.raft.recovery_gates`
gauge list it. `POST /__ursula/raft/{group}/recovery/accept-unsynced-loss`
opens the gate on one replica, recording `initialized`; run on the gated
replicas with the longest logs until the open ones are a majority, an
election then needs a candidate whose log is at least as long as theirs.

Bootstrap follows the log state. A replica that ever held its group never runs
`Initialize`. The group's initializer that holds nothing runs it only when
every configured voter answers the bootstrap probe with an empty group and no
leader, after opening its own gate so the membership entry records
`initialized`.

The journal header version is the format epoch, 3 since the single Raft WAL
work. There is no migration: journals of epochs 1 and 2 and files without the
Ursula WAL magic are refused, and the data directory's `FORMAT_EPOCH` marker
refuses an older directory before any journal is opened.

## Online reclaim and the single-file question

The previous journal was logically purged but physically append-only for the
lifetime of a process. It compacted only during restart, so a long-running node
could retain every historical WAL frame in one growing file even while S3 and
the in-memory state had already converged.

The v1 writer performs an online generation checkpoint after a purge or
truncate once the core journal reaches 64 MiB:

1. append and sync the purge/truncate record;
2. replay the live journal into the current live state of every group,
   checking that it holds exactly the bytes the writer wrote;
3. write and sync the next checksummed generation containing only that state,
   with each group's entries in Append frames of about 8 MiB, so a group's
   live log of any size stays below the 512 MiB frame limit;
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

`raft.wal.fsync` chooses when appends reach stable storage. The default is
`never`: a replica that lost acknowledged appends rejoins through the
recovery gate, so no acknowledged write is lost while at most a minority of a
group's voters lose their unsynced tail.

- `always`: a batch is acknowledged after its `fsync`. The writer collects a
  group commit: it keeps collecting while requests keep arriving, each within
  200 µs of the last, for at most 1 ms after the first and up to 1,024
  requests, so one `fsync` covers a burst however its senders are scheduled.
- `never`: the writer takes what is queued when it wakes, without waiting, and
  acknowledges once the batch is in the page cache. A process crash loses
  nothing. A host crash can drop the unsynced tail, which the run state then
  reports.

Under either policy votes and log states are acknowledged only after
the metadata file is replaced with an `fsync`, and the generation rewrites,
the run state and a clean shutdown `fsync` as well. A graceful shutdown stops
the Raft groups, closes every core writer (each `fsync`s its journal), and only
then records `clean`. If the shutdown grace period expires first, the process
exits without it and the next start reads the run as a crash.

Under `always`, acknowledged application writes retain the existing quorum
contract: append completion is tied to the durable WAL flush callback. Purge
also remains synchronous because the online generation checkpoint may
physically discard the entries it covers.

Committed and truncate markers are replay hints, matching OpenRaft's `log-wal`
contract. They are written immediately but do not request their own fsync. The
next append or purge flushes them with its durable batch. If a crash loses
one first, the durable entries remain and OpenRaft re-establishes the committed
or conflict-truncation boundary after restart. A fresh state machine never
blindly applies the durable suffix: it restores the latest persisted snapshot,
then OpenRaft applies entries as their committed boundary is known. The explicit
committed-tail restart test proves an uncommitted suffix is not exposed as
application state, and the complete OpenRaft log-store conformance suite covers
the storage contract.

## Metrics and operational interpretation

The runtime metrics snapshot now exposes bounded-cardinality core/group WAL
counters:

- `wal_fsyncs` and `wal_fsync_records` show actual physical flush count and the
  number of logical records sharing those flushes. A metadata replacement
  counts two `fsync`s, the file and its directory;
- `wal_reclaims`, `wal_reclaimed_bytes`, and `wal_reclaim_ns` show online
  checkpoint frequency, effect, and cost, and `wal_reclaim_failures` counts
  checkpoints that failed and left the journal unchanged;
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

Restart observability adds per-core `wal_recovery_ns`,
`wal_recovery_records`, `wal_recovery_bytes`, and
`wal_recovery_live_entries`. The implementation has one active generation per
core and no separate payload cache, so segment and cache metrics would describe
objects that do not exist; retained entries are bounded by OpenRaft
snapshot/purge policy instead.

Disk WAL monitors available space every second. Below
`raft.wal.min_available_size`, writes receive `503 WalDiskPressure`,
`/__ursula/ready` returns `503`, elections are disabled, and leaders are
offered to healthy voters. Admission clears only after
`raft.wal.resume_available_size`, providing hysteresis; a stat failure fails
closed. Helm defaults these watermarks to 512 MiB and 1 GiB and keeps
`raft.storageMode=logDir` with a per-pod PVC. Multi-peer memory WAL now requires
the explicit `allow_volatile_multi_peer` development/benchmark/chaos opt-in.

`scripts/soak_raft_wal.sh` first writes past the real 64-MiB reclaim threshold
and proves purge/checkpoint converges the physical file to the live generation.
It then repeats the real-process disk-WAL coverage for a three-voter
replication path, snapshot/purge plus late-learner installation, and durable
restart. It records the exact revision, toolchain, kernel, filesystem,
topology, commands, and per-cycle output under `target/raft-wal-soak`.

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
