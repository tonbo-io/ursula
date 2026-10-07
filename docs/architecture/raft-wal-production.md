# Production Raft WAL with an in-memory state machine

Status: accepted, including the operational rollout gate tracked by
[#273](https://github.com/tonbo-io/ursula/issues/273). The `fsync` policy,
removal of the memory backend, and the journal hardening that follows are
specified in [Single Raft WAL with an fsync policy](single-raft-wal.md).

## Decision

Ursula keeps its deterministic in-memory stream state machine and its existing
per-core, cross-group WAL writer. It adopts the useful properties demonstrated
by OpenRaft's `raft-kv-log-wal-sm-mem` and `log-wal` examples without adopting
one independent WAL worker per Raft group. The journal is segmented, and each
group keeps an index of its entries and a bounded cache of recent ones rather
than every retained entry.

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

Each active core owns a directory `core-N` holding its journal as a run of
segment files, `journal-<sequence>.seg` with a 20-digit sequence, plus
`journal.meta` (below) and the lock `journal.lock`. Sequences are consecutive;
the first segment is 1. Format epoch 3 starts every segment with a 32-byte
header (magic, version, header length, the segment's sequence and a CRC32 of
the header) and encodes every record as:

```text
u32 payload length | u32 header CRC | u32 payload CRC | MessagePack payload
```

The header CRC covers the segment's sequence and the length, so a damaged
length is detected before replay trusts it. The payload CRC covers the
sequence, the length and the payload, so a frame verifies only in the segment
that wrote it, and a segment whose header names another sequence than its file
name is refused.

Appends go to the newest segment. Once it reaches `raft.wal.segment_size`
(64 MiB by default) after a batch, the writer rotates: it `fsync`s the
segment, then creates the next one and makes its header and directory entry
durable before any frame goes to it. Every older segment is therefore complete
on disk under either fsync policy, and a crash can only cut the newest one
short.

Replay reads the segments in order and has two modes:

- `Strict` tolerates only an incomplete final frame of the newest segment,
  which it truncates, and a newest segment without a whole header (a rotation
  a crash cut short), which it removes. Any other frame that fails
  verification, an incomplete frame at the end of an older segment, or a
  missing sequence fails recovery with the segment, frame number and offset.
- `VerifiedPrefix` keeps the frames before the first one that fails
  verification, in whatever segment, and drops the rest of that segment and
  every later segment. It removes the later segments and `fsync`s the
  directory before it truncates that segment, so a second crash cannot bring
  them back after a shorter segment and replay them over a hole.

Startup recovery picks the mode from the run state (below). A journal read as a
verified prefix is always rewritten: each kept segment is copied to a new file
with the same contents, synced and renamed over the old one, so every frame it
keeps is on disk before the core writes again, even a frame whose `fsync`
failed and that only the page cache still held. Copying keeps every frame's
position, so the index replay built stays valid.

The writer is fail-stop. A failed write or `fsync` of a segment, of a rotation
or of a directory, or a segment frame that fails verification when reclaim
reads it, poisons the core's writer for good: the failing request and every
later one fail with `WriterPoisoned`, and the writer never appends after a
possibly partial frame or retries an `fsync` whose dirty pages the kernel may
already have dropped. The process then aborts, because only a restart can
re-read what is really on disk. Deterministic simulation keeps the poisoned
writer instead, so it can observe it and restart the node.
Both modes refuse unknown versions, a damaged segment header, oversized frames
and frames whose checksums verify but whose payload does not decode. The
decoder bounds a single frame at 512 MiB before allocating. No write depends on
reaching that bound, because records are written as OpenRaft hands them over
and rewrites copy small chunks.

The writer holds an exclusive advisory lock in `journal.lock`. A second
process receives a diagnostic error naming the journal, lock path, and recorded
owner PID instead of concurrently modifying the same WAL. The node also holds
`wal.lock` at the WAL root for the whole run, so a second process never
touches the run state.

### Index and entry cache

A group's log in memory is its purge and committed markers, an index of every
live entry (its log id and the segment, offset and length of the frame holding
it), and a cache of its most recent entries. The cache is a contiguous run that
ends at the newest entry, evicted oldest first once it exceeds the group's
share of `raft.wal.cache_size` (256 MiB per node by default, split evenly
between the node's groups). Memory per group is therefore the index, a few
dozen bytes per retained entry, plus the cache, whatever the retained entries
weigh.

The writer is the only one that changes a group's log. It applies a record
after the batch holding it is written (and `fsync`ed when the policy needs it)
and before it replies, so a store sees its own writes, and it validates each
record against the log first, so a record the log would refuse is never
written. Reads come from the log: the log state and the markers from memory,
cached entries from the cache, and older entries from disk. A disk read runs
on Tokio's blocking pool, never on the core's async tasks: it opens the
segment, reads each frame at its recorded position, verifies it as replay
would and checks every entry against the log id the index holds for it. A
limited read, which replication uses, reads at most 8 MiB from disk at a time.
OpenRaft's key log ids come from the index without reading any entry.

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

`rejoin` holds one gate per group and replica. A gate starts from the group's
log state: open when `initialized`, closed when `recovering`, and closed with
an unknown history when `empty` (the replica never held the group here, or
held it on a disk it lost).

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
barrier's committed index. Inbound replication alone never opens it. The
gate first records `initialized`, then opens, and the driver
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

## Reclaim

A purge or truncate makes a reclaim pass due, as does a rotation. The pass runs
after the batch is acknowledged, so it never fails a batch whose writes are
already durable, and it plans without I/O from each group's live bytes per
segment:

1. Every sealed segment older than the oldest one any group needs is deleted,
   oldest first, and the directory is `fsync`ed.
2. When the journal holds more than twice the live bytes of its groups (and at
   least four segments), the oldest remaining segment is freed. A group that
   holds at most a quarter of a segment live there has those records copied
   into the newest segment: its entries in chunks of at most a sixteenth of a
   segment (1 MiB at most), newest chunk first, and its committed and purge
   markers. At most an eighth of a segment is copied per pass, so a pass never
   holds the writer for long. The copies are `fsync`ed before the group's
   index points at them, and the segment is deleted by a later pass once
   nothing points at it.
3. A group holding more than that in the oldest segment is reported lagging:
   its snapshots fell behind, and copying its log forward would only move the
   problem. The snapshot driver reads the lagging groups, wakes as soon as the
   set changes, and snapshots them ahead of its cadence, so their purge frees
   the segment.

Deletion never has to be durable for the journal to stay correct. A deleted
segment that a power loss brings back replays history that later records
supersede: copies of a group's records always follow the originals, and the
latest markers are always written again when they are copied. Replay of a
journal whose old segments are gone sees copied entries after newer ones, so
it allows a gap while it reads and checks that each group's log is consecutive
once every segment is read.

Reclaim errors are classified by what they leave on disk:

- A segment that cannot be opened for a rewrite or removed leaves the journal
  as it was. The pass stops, the failure is logged and counted in
  `wal_reclaim_failures`, appends continue, and a later pass retries.
- A failed write or `fsync` of a copy, a failed directory `fsync` after a
  removal (later segments rely on that directory's durability), or an old frame
  that fails verification poisons the writer: the journal may now differ from
  what the writer believes it holds.

## Space and memory bounds

The journal holds about twice the groups' live bytes and at least four
segments, plus the segment being written. Live bytes are what the snapshot
cadence leaves unpurged, bounded by its node log budget. Restart scan work is
bounded the same way.

Process memory for the Raft log is the index of every retained entry plus each
group's cache, instead of every retained entry. Before this, the journal kept
every retained entry in memory and an online reclaim replayed the whole journal
into a second copy of every group's live state each time a purge left it above
64 MiB. In the memory soak (3 nodes, 32 groups, 2 cores, 128 owners) that was
a reclaim per snapshot, each reading 150 to 320 MB, and node RSS of 2.2 to
3.9 GB.

The `wal` workload of `ursula-state-probe` measures the bound: 16 groups on
one core append 7 KiB entries and purge as snapshots would. With 374 MB
retained, the previous journal held 405 MB of heap and peaked at 1.16 GB during
reclaim. The segmented journal holds 75 MB with 4 MiB caches per group, and
13 MB with 512 KiB caches: about 85 bytes of index per retained entry plus the
caches. The PR state-growth gate runs the workload and fails when the heap
exceeds the caches plus 16 MiB.

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
the metadata file is replaced with an `fsync`, and rotations, reclaim copies,
the run state and a clean shutdown `fsync` as well. A graceful shutdown stops
the Raft groups, closes every core writer (each `fsync`s its journal), and only
then records `clean`. If the shutdown grace period expires first, the process
exits without it and the next start reads the run as a crash.

Under `always`, acknowledged application writes retain the existing quorum
contract: append completion is tied to the durable WAL flush callback. Purge
also remains synchronous because reclaim may delete the segments holding the
entries it covers.

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

The runtime metrics snapshot exposes bounded-cardinality core and group WAL
counters:

- `wal_fsyncs` and `wal_fsync_records` show actual physical flush count and the
  number of logical records sharing those flushes. A metadata replacement
  counts two `fsync`s, the file and its directory, and rotations and reclaim
  passes add theirs;
- `wal_physical_bytes` and `wal_segments` report each core journal's size and
  segment count, summed across cores, and `wal_rotations` counts rotations;
- `wal_reclaims` and `wal_reclaimed_bytes` count deleted segments and their
  bytes, `wal_reclaim_ns` the time reclaim passes took, and
  `wal_reclaim_failures` the passes that stopped on an error that left the
  journal correct;
- `wal_rewritten_bytes` counts live entry bytes copied out of old segments;
- `wal_lagging_groups` and `wal_pinned_segments` report the groups reported
  lagging and the sealed segments kept only for them;
- `wal_cache_hits` and `wal_cache_misses` count entries read from the cache and
  from disk, and `wal_disk_reads` and `wal_disk_read_bytes` the frames read;
- `wal_cache_bytes` and `wal_indexed_entries` report each group's cache and
  index;
- the existing `wal_batches`, `wal_records`, `wal_write_ns`, and `wal_sync_ns`
  continue to report logical store activity and latency.

`wal_fsync_records / wal_fsyncs` is the effective physical batch size. During a
steady workload with snapshots and purge, `wal_physical_bytes` should stay
within about twice the live log, and `wal_reclaims` should grow with it. A
growing `wal_pinned_segments` with a non-zero `wal_lagging_groups` means some
groups' snapshots fall behind. A monotonically growing `wal_physical_bytes`
with zero `wal_reclaims` means the snapshot driver or its safety prerequisite
is stalled. `wal_cache_misses` stays near zero while every follower keeps up;
it rises while a follower catches up from disk.

Restart observability adds per-core `wal_recovery_ns`,
`wal_recovery_records`, `wal_recovery_bytes`, and
`wal_recovery_live_entries`.

Disk WAL monitors available space every second. Below
`raft.wal.min_available_size`, writes receive `503 WalDiskPressure`,
`/__ursula/ready` returns `503`, elections are disabled, and leaders are
offered to healthy voters. Admission clears only after
`raft.wal.resume_available_size`, providing hysteresis; a stat failure fails
closed. Helm defaults these watermarks to 512 MiB and 1 GiB and always gives
each voter pod a `raft-data` PVC at `raft.logDir`. The disk journal is the only
Raft WAL: a static cluster requires `raft.wal.path`, and a single node without
one keeps its journals in a temporary directory removed on clean shutdown.

`scripts/soak_raft_wal.sh` repeats the real-process disk-WAL coverage for a
three-voter replication path, snapshot/purge plus late-learner installation,
and durable restart. It records the exact revision, toolchain, kernel,
filesystem, topology, commands, and per-cycle output under
`target/raft-wal-soak`.

## Verification envelope

The storage increment is covered by:

- OpenRaft's complete log-store conformance suite against
  `RaftGroupFileLogStore`;
- checksum corruption, unknown format, oversized frame, and torn-tail tests,
  and positioned reads that verify as replay does;
- rotation, purge deleting segments, the chunked rewrite of a quiet group, the
  lagging report, recovery across segments in both modes, a torn tail in the
  newest segment, corruption and a missing segment in an older one, and cache
  eviction with disk reads returning identical entries;
- exclusive-owner, snapshot/purge/truncate recovery and committed-suffix
  restart tests;
- deterministic simulation on the simulated disk: rotation, purge and rewrite
  while nodes crash within the durability contract, a lagging follower caught
  up from the leader's disk, injected write, `fsync` and removal failures, and
  a strict replay of the segment scenario under `check_determinism`;
- the `wal` workload of `ursula-state-probe`, which holds the WAL heap to the
  index and the caches with retained log far above the cache budget;
- the existing late-learner, S3 cold-manifest, and durable multi-node tests.

The format change does not require an OpenRaft upgrade. Ursula remains pinned to
its current patched OpenRaft release so storage-format risk and consensus-library
risk are evaluated independently.
