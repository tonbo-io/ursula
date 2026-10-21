# Raft WAL

## Overview

Each node keeps its Raft logs in a disk journal per core, shared by every Raft
group that core owns. The stream state machine stays in memory and is rebuilt
from it:

```text
persisted group snapshot + committed Raft WAL suffix -> in-memory state machine
hot stream tail + immutable S3 chunks                -> Durable Stream history
```

These are two different logs. S3 compaction makes old payload available
outside the hot ring but never authorizes deleting a Raft entry. The journal
reclaims a group's entries only after OpenRaft persisted a snapshot and
advanced the group's purge boundary.

A core journal has one writer, a loop on a dedicated OS thread with a
current-thread runtime, so blocking file I/O stays off the async workers. One
write and one `fsync` cover the records of many groups.

Each group's snapshot pointer, `core-N/group-G.snapshot.json`, is published
through the journal's I/O seam: temporary file, sync, rename, directory sync.
After a rename whose directory sync failed, a process restart may see the new
pointer and a host restart the old one. Recovery accepts either.

## Configuration

| Key | Default | Meaning |
| --- | --- | --- |
| `raft.wal.path` | none | WAL directory. Journals live in its `raft-log` subdirectory. Required when `raft.peers` is set. |
| `raft.wal.fsync` | `never` | When appends reach stable storage: `always` or `never`. |
| `raft.wal.segment_size` | 64 MiB | Size at which a core journal starts a new segment, at least 4 KiB. |
| `raft.wal.cache_size` | 256 MiB | Recent entries kept in memory per node, split evenly between its groups. |
| `raft.wal.min_available_size` | 512 MiB | Free space below which the node refuses writes. Zero disables the guard. |
| `raft.wal.resume_available_size` | 1 GiB | Free space at which the guard clears. Must exceed the minimum. |

There is no interval `fsync` mode: after a host crash under `never` a replica
keeps only its verified prefix and rejoins through the recovery gate, so a
periodic `fsync` would not change recovery.

The zero-config development mode (the `default` preset on one node) runs
without Raft. Any other single node without `raft.wal.path` uses a temporary
directory, removed only after a clean WAL shutdown. The Helm chart sets the
path to `raft.logDir` on each voter's `raft-data` PVC and exposes the other keys
as `raft.walFsync`, `raft.walSegmentBytes`, `raft.walCacheBytes`,
`raft.minAvailableBytes` and `raft.resumeAvailableBytes`.

## On-disk layout and format

| Path under `{raft.wal.path}/raft-log` | Content |
| --- | --- |
| `FORMAT_EPOCH` | Format-epoch marker of the data directory. |
| `wal.lock` | Advisory lock held by the node for the whole run. |
| `run-state.bin` | Boot id, root device and status of the last run, and the `fsync` policy and recovery epoch of each journal class. |
| `topology.bin` | Core count, group count, cluster id, node id and root id the root was created with. |
| `identity.bin` | The run counter of the root, and whether a run read a predecessor that may still run. |
| `core-N/journal-<seq>.seg` | Journal segments, 20-digit consecutive sequences starting at 1. |
| `core-N/journal.meta` | Every group's vote, log state and log generation, and the core's verified recovery epoch. |
| `core-N/journal.lock` | Advisory lock held by the core's writer. |
| `core-N/frozen-<group>-<seg>-<offset>-<first>-<last>-<hash>.seg` | Entry archives of apply-stopped groups (see Reclaim). |
| `meta/` | The meta group's journal, laid out as a core's, with its snapshot record `group-0.snapshot.json`. |

A second process that tries to take either lock fails with an error naming the
lock path and the owner PID. `topology.bin` binds the root to its routing, its
cluster and its node: counts that differ from the stored ones refuse startup, a
configured `server.cluster_id` that differs from the stored one refuses startup
too, so does a `raft.node_id` other than the one that created the root, and so
does prior state (a run state, a core directory or a meta journal) without a
topology file. A root created without a configured id stores the id the node
generated.

A new root also draws its root id, 64 random nonzero bits written as 16
lowercase hex digits, which names the disk to the meta group. Every start
counts itself in `identity.bin`, synced, before it records itself `running` and
before the node binds its peer identity. A process is named by its
incarnation, the root id, the run counter and a 32-bit nonce drawn at start, in
32 hex digits: `x-ursula-process-incarnation` and `process_incarnation` in
`GET /__ursula/metrics` carry it. A later run of a root names itself above
every earlier one. A node without a WAL draws a root id per start and runs as
run 0. A root that holds prior state without `identity.bin` refuses startup,
since a counter that started again would make a later run read as an earlier
one, and so does a counter at its last value. An empty disk is the way back.

Every segment starts with a 32-byte header, and every record is a frame:

```text
header  magic "URSJWAL\0" | u16 version | u16 header length (32) | u32 zero
        | u64 sequence | u32 zero | u32 CRC32 of the 28 bytes before it
frame   u32 payload length | u32 header CRC | u32 payload CRC | compact MessagePack payload
```

The frame header CRC covers the segment sequence and the length, so a damaged
length is caught before replay trusts it. The payload CRC covers the sequence,
the length and the payload. A frame therefore verifies only in the segment
that wrote it, and a header naming another sequence than its file is refused.
A payload is the array `[group id, generation, record]`, in compact
MessagePack (`ursula_stream::msgpack`): a struct is an array of its fields in
declaration order and a variant keeps its name. The record is one of
`Append`, `FrozenAppend`, `SaveCommitted`, `TruncateAfter` or `Purge`, and the
generation is that of the group's log the record belongs to. A frozen archive
holds one such payload. Votes are not journal records.

`journal.meta`, `run-state.bin`, `topology.bin` and `identity.bin` are replaced whole
(temporary file, sync, rename, directory sync), so a crash leaves either
version. Each carries a magic, the format epoch, a length and a CRC32, and a
damaged one fails startup. `journal.meta` records each group's log state:

- `empty`: the replica never persisted an entry or a purge of the group.
- `initialized`: its log holds every entry it acknowledged.
- `recovering`: its log may be missing acknowledged entries. The recovery gate
  stays closed until the state returns to `initialized`.
- `retired`: the replica's log of the group was torn down, and the entry
  names the next generation with no vote and no recovery plan. It counts as
  initialized. The format holds this state, but nothing writes it yet.

A group leaves `empty` in the batch that persists its first entry or purge and
never returns to it. That write records `recovering` instead of `initialized`
when the group's gate is closed and the replica holds nothing of it.

Each group's entry also names its log generation, 0 until the replica's first
retirement of the group and one more after each. A core journal interleaves
the frames of its groups, so the frames of a retired log cannot be erased in
place. Every journal record and every snapshot pointer therefore names its
generation, and only what belongs to the generation `journal.meta` names is
read back. Older frames are dead bytes that reclaim frees, and a pointer of
another generation restores nothing. Without `journal.meta`, replay keeps each
group's highest generation among its frames, the group opens `recovering`,
and the rewritten `journal.meta` names that generation. Every group stays at
generation 0 until replica teardown writes `retired`.

`run-state.bin` holds one record per journal class, each with the class's
`fsync` policy and recovery epoch, beside the node-wide boot id, root device and
status.
The data class covers the core journals and runs the configured policy. The
meta class always runs `always` and covers the meta journal under `meta/`.
Every node that runs Raft opens it at start, before its runtime starts and so
before any data group of the run opens. A meta journal that does not open
leaves the node without a meta replica and never stops the start.

The journal and state-file version is the format epoch, 6. There is no
migration: other epochs and files without the Ursula magic are refused, and the
`FORMAT_EPOCH` marker refuses a directory of another epoch before any journal
opens. The decoder bounds a frame payload at 512 MiB and a state file at 64 MiB
before allocating. No write approaches the frame bound, because records are
written as OpenRaft hands them over and rewrites copy small chunks.

## Write path and fsync policy

An append publishes its entries as a readable pending batch and queues the
record to the core writer without waiting for I/O. The writer validates each
record against the group's log (a record the log would refuse is never
written), writes the batch, syncs it when required, applies each record to the
group's in-memory log and then completes OpenRaft's `IOFlushed`.

- `always`: a group commit. The writer keeps collecting while requests arrive
  within 200 µs of the last, for at most 1 ms and 1,024 requests, then
  acknowledges after one `fsync`.
- `never`: the batch is whatever is queued when the writer wakes, acknowledged
  once it is in the page cache.

Under either policy, a batch with a membership entry or a `FrozenAppend` is
synced before acknowledgement, because losing every copy of the bootstrap
membership leaves no voter set to recover from. Votes and log states are
acknowledged only after `journal.meta` is synced. Rotations, reclaim copies,
the run state and shutdown sync as well.

Committed and truncate markers are replay hints, never synced on their own. If
a crash loses one, OpenRaft re-establishes the boundary after restart. A fresh
state machine restores the latest snapshot and applies entries only as their
commit becomes known, so an uncommitted suffix is never application state.

A group's snapshot pointer (`core-N/group-G.snapshot.json`) is synced on its
own, while a truncate marker waits for the core's next synced batch under
either policy. So the journal is synced before the pointer is published, for a
snapshot the replica installed and one it built. A durable pointer therefore
implies a durable truncate of any uncommitted tail the snapshot replaced. Each
snapshot publication costs one journal `fsync`. At startup, before it reads the
snapshot's body, a replica refuses to open a group whose log holds an entry, or
whose commit marker names one, at or after the restored snapshot's index but
below the snapshot in log-id order. It reports `LogBelowSnapshot`.

Once the newest segment reaches `segment_size`, the writer rotates: it syncs
the segment, then creates the next one and makes its header and directory entry
durable before any frame goes to it. A crash can therefore only cut the newest
segment short.

## Durability contract

| Failure | `always` | `never` |
| --- | --- | --- |
| Process crash, same boot (OOM, panic, kill) | No loss | No loss: the page cache holds every write |
| Graceful shutdown | No loss | No loss: shutdown syncs every journal |
| One voter's host crashes or loses power | No loss. A complete invalid frame in a core's newest segment sends that core's groups through the recovery gate. | That voter keeps its verified prefix and rejoins through the recovery gate. The other voters hold every acknowledged write. |
| A majority of voters crash at once | No loss. If a complete invalid frame is cut on a majority of a group's voters, the group stays stopped until an operator accepts a loss, although none happened. | Writes acknowledged within the unsynced window can be lost. The group stays stopped until an operator accepts that loss. |
| One voter loses its disk | Rejoins empty through the recovery gate | Same |

`never` turns simultaneous host crashes of a quorum from an outage into a
bounded loss of the latest writes. Choosing it is an explicit operator decision.

`always` can still need the operator call. The newest segment can hold bytes
written after the last `fsync` (an unacknowledged batch or a marker), and a host
crash can leave a complete frame there that fails verification. Recovery cannot
tell that from media corruption of an acknowledged write, so it gates every
group on that core before cutting the frame. A single-voter group, or a group
with a majority of voters gated together, then waits for the operator.

Graceful shutdown hands off leadership (at most 5 s), drains listeners (at most
10 s), stops the Raft groups, closes every core writer (each syncs its journal)
and only then records `clean`. A second signal, or a shutdown still running 20 s
after the first signal, exits at once without `clean`, so the next start treats
the run as a crash.

## In-memory index and cache

A group's log in memory is its purge and committed markers, an index of every
live entry (log id plus segment, offset and length of its frame) and a cache of
its newest entries, evicted oldest first beyond the group's share of
`cache_size`. Memory per group is the index, a few dozen bytes per retained
entry, plus the cache.

Markers, cached entries and pending entries are read from memory, older entries
from disk on Tokio's blocking pool. A disk read verifies each frame as replay
would and checks every entry against the indexed log id. A segment that reclaim
deleted between planning and opening a read is planned again, up to four times,
because the rewrite that freed it moved the entries first. A limited read, used
by replication, returns the longest prefix weighing at most 4 MiB and never less
than one entry. OpenRaft's key log ids come from the index.

## Startup and replay

The node takes `wal.lock`, reads `run-state.bin`, checks `topology.bin` and
`identity.bin`, reads every core's `journal.meta` and decides how to open the
journals. Before any journal write it durably records itself `running` with the
current boot id from `/proc/sys/kernel/random/boot_id` (the host's, inside a
container) and the device the root is mounted from (`st_dev`). Without a boot
id, a run that did not record `clean` reads as a host crash. On the same boot,
another device means the volume was mounted anew, whose page cache is not the
old mount's, so the run reads as one after a host crash.

| Previous run | Replay | Node recovery state |
| --- | --- | --- |
| No run state, no journal record, no verified epoch | Strict | normal |
| No run state, a journal record or a verified epoch survives | Verified prefix | recovering (unknown history) |
| `clean` | Strict | normal |
| `running`, same boot id and device (process crash) | Strict | normal |
| `running`, same boot id, other device (remounted), policy `always` | Strict | normal |
| `running`, same boot id, other device (remounted), policy `never` | Verified prefix | recovering (remounted) |
| `running`, other or unknown boot id (host crash), policy `always` | Strict | normal |
| `running`, other or unknown boot id (host crash), policy `never` | Verified prefix | recovering (host crash) |
| `poisoned` | Verified prefix | recovering (poisoned) |

The previous run's recorded policy applies, even if configuration changed. In
both modes sealed segments are read strictly: invalid or incomplete sealed
frames, damaged headers and missing sequences fail startup without changing the
journal. The newest segment keeps the prefix before its first frame that fails
verification. Replay never skips a bad frame, because writeback after a host
crash can persist later pages before earlier ones. An incomplete final frame or
torn newest header is dropped without gating. Before cutting a complete invalid
frame, recovery durably marks every group in that journal `recovering`.

A verified-prefix read, or one that cut an invalid frame, copies every kept
segment to a new file with the same contents, syncs it and renames it over the
old one. Frames whose `fsync` failed are then on disk, and the index stays valid.

Each verified-prefix run begins a recovery epoch above both the run state's
epoch and every core's verified epoch, so a lost run-state file cannot restart
the count. Cores open lazily, and a core whose verified epoch is older is still
read as a verified prefix, however the runs in between ended. An epoch that
cannot advance or unreadable metadata refuses startup.

A recovering run marks every `initialized` group `recovering` before recording
itself `running`, so the per-group state carries recovery across later crashes
and clean shutdowns until each gate opens. A group with journal entries but
`empty` metadata is repaired as `initialized`, or as `recovering` when the node
is recovering, the tail was cut or `journal.meta` is missing. An `initialized`
group whose journal holds nothing of it lost its log and becomes `recovering`.

A run that records `always` after a `never` run whose process crashed on this
boot first syncs every core's newest segment and directory, because that run
acknowledged writes only the page cache held. A host crash before the new run
records itself still reads as one under `never`.

The start also reads whether a process of an earlier run of the root may still
run, its predecessor, and reports it as `predecessor` in `wal_recovery` of
`GET /__ursula/metrics`. The first matching row decides:

| At open | Predecessor |
| --- | --- |
| The root is empty: no core or meta journal holds a record, no core or meta metadata holds a vote, and no snapshot record exists | `may_live` |
| `identity.bin` records a predecessor that may live | `may_live` |
| `clean`, `poisoned`, or `running` on the same boot and device | `gone` |
| Remounted, host crash, no run state | `may_live` |

A start that reads `may_live` records it in `identity.bin` before it records
itself, and the record stays across restarts, a clean one included.

## Fail-stop

A failed write or `fsync` of a segment, rotation, rewrite copy or directory, or
a frame that fails verification when reclaim reads it, poisons the core writer.
That request and every later one fail with `WriterPoisoned`. The writer never
appends after a possibly partial frame and never retries an `fsync` whose dirty
pages the kernel may have dropped. It tries to record `poisoned` (best effort,
the same disk may refuse) and the process aborts so a restart re-reads what is
on disk. Deterministic simulation keeps the poisoned writer to observe it.

If a failed journal `fsync` and a failed `poisoned` record are followed by a
restart and then a power loss, clean-but-lost pages can make the history
unreadable. Startup refuses it without truncating the acknowledged suffix, and
the replica is restored from a healthy copy.

## Recovery gate

The gate (`rejoin`) keeps a replica whose log may lack acknowledged entries from
electing a leader that lacks them. Each group replica has one. It starts open
for `initialized`, closed for `recovering`, and closed with unknown history for
`empty`. Its status is `open`, `awaiting_barrier`, `catching_up` or `stalled`.

**While closed** the replica does not campaign and refuses a transfer to
itself. It refuses every vote once it knows the group holds entries: its log
state says so, a leader reported a commit index of 1 or more, or a candidate's
log reached index 1. Candidates whose log is only the membership entry still
pass, so a new group's first election works. It accepts appends from any leader
whose vote is not lower than its persisted vote. An older leader gets
`HigherVote`.

**Vote demotion.** OpenRaft restores a committed vote for itself as leadership
without an election, and a replica that lost entries as leader would reuse their
log ids and fork the group. After any run that did not end `clean`, and whenever
its gate starts closed, the replica rewrites its own committed vote as the same
vote uncommitted, synced to `journal.meta` before its Raft core starts. It starts
as a follower. A failed write stops the group. Votes for other replicas are
never changed.

**Opening.** The barrier driver asks the current leader for a fresh ReadIndex
barrier (`RejoinBarrier`) and opens the gate once the replica applied the
barrier's index. That index is the higher of the read index and the leader's
last log index read after the round (`confirm_recovery_barrier`). The read
index alone can be too low: an acknowledgement the replica's previous process
sent before its host crashed can reach the leader after the round and count
the replica's lost copy of an entry toward its commit. Within one leadership
such an acknowledgement names no entry past the leader's last log index, so a
replica that applied that index holds every entry it could count. Inbound
replication alone never opens the gate. The gate records `initialized`, opens,
and the driver refreshes the election policy.

**Rewind in place.** A follower that answers `Conflict` at or below the index it
had matched in this leadership lost entries. Every group with a recovery gate
runs OpenRaft with `allow_log_reversion`, and the network passes every conflict
to OpenRaft unchanged. The leader resets its progress for that follower on the
first such conflict, from a heartbeat or a replication request, and replicates
its log to it again. The follower stays a voter, and no membership entry is
proposed. A follower checks the leader's vote before its log, so its conflict
still counts as its acknowledgement of the leader. The leader keeps its quorum
and its lease while the follower catches up, even when the followers that lost
entries are a majority, as long as they kept their vote. A replica that lost
its vote, such as one on a wiped disk, answers errors until a quorum without it
proves a vote floor, so two such replicas of three never heal on their own.
This is safe because the leader holds every committed entry and a replica that
may have lost entries votes for no one until its gate opens.

The heal driver observes each reversion in the leader's replication metrics. It
logs a warning that names the group and the follower and counts it per
follower in `ursula.raft.follower_log_reversions`. A follower that lost entries
without a host crash or a lost disk, for example on a disk that drops synced
writes, shows up there. The driver also adds a configured voter that is missing
back as a learner, promotes it once it caught up, and flattens a joint
configuration whose second step never committed. Desired voters come from the
static configuration.

**Lease while followers catch up.** Gated followers renew the leader's lease
before they caught up. If they cannot catch up, for example when one needs a
snapshot while the snapshot store is unavailable, the leader stays live
(`leads_live`) and a recovery plan refuses to fence the group. Writes are then
accepted and wait for a commit, and they can time out with an unknown outcome
instead of being refused before the proposal.

**Handoff.** A leader hands off only to another voter that acknowledged it
within one minimum election timeout (1.5 s) and holds every committed entry
(`ElectionPolicy::validate_handoff`, used by every planner and the admin
transfer endpoint). A follower the leader rewound is refused as lagging until
it caught up. If the target has not taken over within two maximum election
timeouts (6 s), the leader steps down and campaigns.

**Stall and accept-unsynced-loss.** A gated replica with no barrier and no
apply for 30 s reports its group stalled: a majority may be gated, so the group
has no leader. Readiness answers `503` with reason `recovery_stalled`. The
operator ranks gated replicas by `last_log_term` and `last_log_index` from the
metrics and calls `POST /__ursula/raft/{group}/recovery/accept-unsynced-loss` on
the longest ones until the open replicas are a majority. The body names
`expected_last_log_index` and `expected_current_term` as seen. The replica
answers `409 Conflict` and changes nothing unless it has a durable vote floor,
its gate is stalled and it still holds that log. An open gate answers `200` with
outcome `already_open`. The admin incarnation precondition applies.

**Bootstrap.** A replica that ever held its group never runs `Initialize`. The
initializer, holding nothing, runs it only when every configured voter reports
an empty group, no leader and a persisted genesis vote floor `(0, 0)`. It opens
its own gate first, so the membership entry records `initialized`. A durable
`(0, 0)` proves the peer never voted or acknowledged a leader, since either
persists a higher vote first.

**Lost vote history.** A replica with no vote, or a missing `journal.meta`
beside surviving records, rejects appends, heartbeats and snapshots until a
leader's ReadIndex barrier, confirmed by a quorum without it, proves a vote that
it persists. `accept-unsynced-loss` cannot replace that proof. A recovering
replica that kept its vote receives replication immediately.

## Reclaim

A purge, truncate or rotation makes a reclaim pass due. The pass runs after the
batch is acknowledged and plans without I/O from each group's live bytes per
segment and from which segments hold dead entries:

1. Delete every sealed segment older than the oldest one any group needs.
2. When the journal exceeds twice its live bytes and four segments, free the
   oldest remaining segment. Groups holding at most a quarter segment live there
   are copied into the newest segment: entries in chunks of a sixteenth segment
   (256 bytes to 1 MiB), newest first, then their markers, at most an eighth
   segment per pass. Copies are synced before the index points at them.
3. Report groups holding more as lagging. The snapshot driver watches that set
   and snapshots them ahead of its cadence, so their purge frees the segment.

**Dead entries age out (#669).** An entry is dead once it is purged, truncated,
written again or copied forward by a rewrite. Its frame stays in its segment
until the segment is deleted. The writer keeps, per segment, since when it holds
a dead entry. The writer has no timer: at most once per drive interval
(`storage.snapshot.drive_interval`), however often the driver ticks, the
snapshot driver sends each core writer a maintenance pass with the driver's time
and the age A, a quarter of `raft.deleted_data_max_age`. The driver runs the
passes in a task of its own and starts the next only once the last one ended,
so a slow writer holds up no snapshot build. The pass stamps the deaths since
the last pass with that time, counting how long the writer waited for it. It
seals the active segment once its dead entries are A old, so that a quiet core
frees it like any other. Then, even when the journal
is within its budget, a kept segment that held dead entries for A frees the
oldest kept segment as step 2 does, under the same quarter-segment rule and
pass budget. A journal gives back only its oldest segment, so the segments
before it are freed first, and their live entries move forward. A group holding
more than a quarter segment there is reported lagging. The driver snapshots it
first, or, when it applied nothing since its snapshot, purges its log through
that snapshot, so that the entries OpenRaft keeps below a snapshot never pin the
segment. These rewrites run only in the driver's passes, at most an eighth
segment per core per drive interval, so a mass removal does not saturate the
disk: 8 MiB per second per core at the default 64 MiB segments and the 1 s
interval of inline snapshots, 1.6 MiB per second with an S3 snapshot store. A
journal without old dead entries is never rewritten for their age, so an idle
core settles after one round. At open, replay finds the dead entries again, and
a segment's age is the time since its modification time (`JournalDisk::modified`,
which the simulated disk tracks too). A dirty newest segment is sealed at open.

A group whose committed application failed is stopped and its WAL kept for
replay by corrected code. It cannot snapshot, so reclaim always rewrites it out
of the oldest segment within the pass budget. Its entries go to an immutable
`frozen-*.seg` archive, synced with its directory, referenced by a
`FrozenAppend` record carrying the archive's length, CRC32, BLAKE3 hash and the
log ids still live in it. A `FrozenAppend` is copied forward as a reference with
only its live log ids, so purged or truncated entries never return. Replay and
reads verify an archive's length, checksums, group and index range. An archive
is deleted only after the segments referencing it are durably gone, and not
before the segments present at a startup that cut a tail are gone.

Before deleting a segment the pass syncs the segment being written, under either
policy, because the record that freed the segment may be only in the page cache.
Otherwise a host crash could keep the deletion and lose the purge, leaving a
hole in the log. Deletion itself need not be durable: a resurrected segment
replays history that later records supersede. Replay tolerates gaps while
reading and checks each group's log is consecutive at the end.

A segment that cannot be opened or removed, or an archive that cannot be
collected, leaves the journal as it was: the pass stops, counts
`wal_reclaim_failures` and a later pass retries. A failed write or `fsync` of a
copy or archive, a failed sync before or after a removal, or an old frame that
fails verification poisons the writer.

## Bounds

**Space.** A journal holds about twice its live bytes, at least four segments,
plus the active one and any archives. Live bytes are what the snapshot cadence
leaves unpurged, bounded by its node log budget. Restart scan work follows.

**Dead entries.** A segment that holds a dead entry is deleted within A, a
quarter of `raft.deleted_data_max_age`, of the pass after the entry died, plus
the passes the copies take at an eighth segment per drive interval, provided the
groups that hold more than a quarter segment in the segments before it are
snapshotted or purged. At open a segment's age is the time since its
modification time, which is at most one A later than its oldest death, and a
dirty newest segment is sealed, so a plain restart keeps the bound within 2A.
An open that rewrites every kept segment (a verified-prefix replay after a host
crash under `fsync = never`, or a journal that ended in an invalid frame) resets
their modification times, and their age then starts at the open. Archives of
apply-stopped groups are not covered.

The age rule has three costs, all visible in `wal_rewritten_bytes`:

- The small live logs of quiet groups are copied forward about once per A.
- Groups whose live log in such a segment passes a quarter segment are reported
  lagging and snapshotted, or purged through their snapshot, about once per A.
- The first passes after an upgrade to 0.8.1 or a restart find every old dead
  entry at once. They copy the live remainders of all kept segments, up to about
  twice the core's live bytes, at the per-interval rate, and snapshot the groups
  reported lagging, before the journal settles.

**Free space.** Free space on the WAL filesystem is sampled every second. Below
`min_available_size`, writes get `503 WalDiskPressure`, readiness answers `503`
with `wal_disk_pressure`, and the node stops campaigning and offers its
leaderships to other voters. It clears at `resume_available_size`. A failed stat
fails closed.

**Memory.** The Raft log costs the index plus the caches, not every retained
entry. The `wal` workload of `ursula-state-probe` retains log far above the
caches and checks that WAL heap, at the end and at peak, stays within the caches
plus 16 MiB. The bounded-state workflow runs it in its pull-request ratchet.

## Metrics and readiness

Per-core and per-group counters:

- `wal_batches`, `wal_records`, `wal_write_ns`, `wal_sync_ns`: store activity.
- `wal_fsyncs`, `wal_fsync_records`: physical flushes and records sharing them.
  A metadata replacement counts two. Their ratio is the effective batch size.
- `wal_physical_bytes` (segments plus archives), `wal_segments`,
  `wal_rotations`.
- `wal_reclaims`, `wal_reclaimed_bytes`, `wal_reclaim_ns`,
  `wal_reclaim_failures`.
- `wal_rewritten_bytes`, of which `wal_stopped_rewritten_bytes` belong to
  apply-stopped groups and `wal_frozen_reference_rewritten_bytes` are copied
  `FrozenAppend` references.
- `wal_lagging_groups`, `wal_pinned_segments` (sealed segments kept only for
  lagging groups), `wal_dirty_segments` (segments that hold dead entries).
- `wal_cache_hits`, `wal_cache_misses`, `wal_disk_reads`, `wal_disk_read_bytes`,
  `wal_cache_bytes`, `wal_indexed_entries`.
- `wal_recovery_ns`, `wal_recovery_records`, `wal_recovery_bytes`,
  `wal_recovery_live_entries`: replay cost at open.

In steady state `wal_physical_bytes` stays near twice the live log and
`wal_reclaims` grows. Growing `wal_pinned_segments` with non-zero
`wal_lagging_groups` means snapshots fall behind. Growing `wal_physical_bytes`
with no reclaims means the snapshot driver is stalled. `wal_cache_misses` rises
only while a follower catches up from disk.

`wal_recovery` in the metrics JSON, the `ursula.wal.recovering` gauge and
`RaftWal::recovery_state` report whether and why the node opened recovering.
`recovery_gates` and the `ursula.raft.recovery_gates` gauge list gated replicas
and stalled groups. `recovery_gates` names a data group by its id and the meta
group as `"meta"`, and gate errors and logs name the plane too, so the meta
group never reads as data group 0. Only data groups make a node unready, so
`recovery_stalled_groups` in `/__ursula/ready` lists data groups only. The `ursula.raft.follower_log_reversions` counter, labelled
by `follower`, counts the followers a leader on this node rewound because they
lost entries they had acknowledged. `/__ursula/ready` names `wal_disk_pressure`,
`recovery_stalled` or `recovery_gate_closed` and reports the watermarks.

## Component boundaries

The data-group engine owns its recovery tasks and exposes the runtime's
`GroupEngine`. Construction takes `RaftGroupEngineOptions`.

| Component | Responsibility and public seam |
| --- | --- |
| `RaftWal` | Node run state, per-core writer lifetime, group log stores and shutdown. Exported from `ursula_raft::wal`, with format inspection types under `wal::diagnostics`. |
| `RecoveryGate::attach` | Binds the gate, publishes the group's resources, starts the bootstrap, heal and barrier drivers and keeps their handles. Engine shutdown aborts and joins them. gRPC and simulation inject a `RecoveryTransport`. |
| `ElectionPolicy` | Combines node leadership shedding with per-group recovery eligibility, drives OpenRaft's election switch and validates handoff targets and incoming transfers. Cold-health pressure alone leaves a node electable, and a node without a meta replica stays electable but takes no transfer. |
| `GroupEntry` | Registry entry per group. `Active` holds the Raft handle, read barrier, recovery gate, apply health and cold-index cache. `ApplyStopped` holds the apply failure and last applied log id and refuses requests. Each RPC keeps one entry through dispatch, and a gate cannot be removed by republication. |
| `ursula-proto` | Shared admin requests, responses and recovery status. WAL metric samples live in `ursula-runtime`. |

All server handoffs enter through the registry. The destination refuses a
transfer to itself while it cannot campaign, and one carrying its own committed
vote, because OpenRaft would start that election while still holding its old
leader state. The check reads only the request's vote, and OpenRaft acts on a
transfer only while the node holds that vote, so nothing slips in between.

Bootstrap waits on server-state and gate notifications, heal on every metrics
change, so it sees a rewound follower's matched index drop, and the barrier
driver also watches apply progress. They keep deadlines for retries and stall
reporting and rate-limit repeated steps. Segment pressure reaches the snapshot
driver through `LaggingGroups`.

`GET /__ursula/raft/{group}/quorum`, which requires the incarnation header,
confirms a fresh ReadIndex round with an unchanged committed leader vote and
returns the group, leader, term and required applied index, the higher of the
read index and the leader's last log index. It is an
observation, not permission to remove a voter. `ursulactl` uses only the admin
protocol and does not depend on `ursula-raft`. The fault-injection `InProcess*`
network is compiled only under `cfg(test)` or `cfg(madsim)`.

## Testing

Every journal file operation goes through `log_store::disk`, implemented by the
OS and, under `cfg(madsim)`, by a simulated disk that keeps synced bytes, drops
or reorders unsynced pages on a host crash, keeps every byte on a process crash
and injects I/O errors. Simulation therefore runs the production journal.

- OpenRaft's log-store conformance suite runs against `RaftGroupFileLogStore`
  on the real and simulated disk.
- Journal unit tests cover corruption, unknown formats, oversized frames, torn
  tails, positioned reads, rotation, deletion, chunked rewrite, lagging reports,
  recovery in both modes, missing segments, cache eviction and frozen archives.
- Simulation covers rotation, purge and rewrite under crashes within the
  contract, catch-up from disk, injected write, `fsync` and removal failures,
  the poison-publication matrix, and strict replay under `check_determinism`.
- Invariants per policy: no acknowledged write lost within the contract, gated
  replicas never vote, majority loss stops the group until accepted.
  Regressions: an accepted leader never forks on restart, a `never` to `always`
  switch across a process crash keeps every acknowledged write or gates, an
  acceptance opens only a stalled gate for the log seen, and a wiped voter never
  lets a stale leader commit.
- `scripts/bench_raft_wal.sh` runs the `disk_wal` criterion benchmark, comparing
  the shared per-core writer with a file per group and an upstream `raft-log`
  adapter per group, and `scripts/summarize_raft_wal.py` summarizes runs.
- `scripts/soak_raft_wal.sh` repeats real-process three-voter replication, a
  voter healed from an empty WAL, and durable restart,
  recording its environment and output under `target/raft-wal-soak`.
