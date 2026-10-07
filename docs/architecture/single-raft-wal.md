# Single Raft WAL with an fsync policy

Status: proposed. It removes the memory WAL backend and extends the per-core
journal described in [Production Raft WAL](raft-wal-production.md).

## Decision

- Ursula has one Raft WAL backend: the per-core shared journal. The memory
  backend is removed. Configurations that select it are refused at startup;
  there is no migration path because no deployment depends on it.
- `raft.wal.fsync` chooses when journal appends reach stable storage:
  - `never` (default): appends are written to the page cache and acknowledged
    without `fsync`.
  - `always`: every batch is acknowledged only after `fsync`, which is the
    current behaviour.

  There is no interval mode. After an unclean crash a replica keeps only its
  verified prefix and rejoins through the recovery gate, so a periodic `fsync`
  would not change what recovery does.
- Raft metadata that must survive any crash is stored outside the journal and
  is always written with `fsync`. That covers each group's vote, whether each
  group was ever initialized, and the node's run state.
- The journal stays an Ursula component. It is hardened and gains the
  raft-engine techniques listed below rather than being replaced by
  raft-engine.

## Why not raft-engine

raft-engine is production-proven in TiKV, and its per-batch `sync` flag matches
this design. It does not fit Ursula today:

- It cannot run under madsim without a fork. It spawns OS threads and a rayon
  pool, and it bypasses its `FileSystem` trait for directory listing, locking,
  directory `fsync` and `statvfs`.
- Its API blocks the calling thread, and its thread-local block cache is keyed
  without the engine identity. Several engines served from one thread can read
  each other's blocks.
- The crates.io release (0.4.2, April 2024) predates fixes Ursula would need,
  such as `Engine::sync`. That means pinning upstream master and carrying a
  fork, along with protobuf 2 and `lz4-sys`.

Ursula ports these techniques instead:

- segmented files with segment-level purge;
- rewriting only live entries out of old segments, with feedback that names
  lagging groups;
- checksums seeded with the segment sequence number;
- tail tolerance limited to the newest segment;
- fail-stop on I/O errors;
- an in-memory index instead of in-memory entries.

## Durability contract

| Failure | `always` | `never` |
| --- | --- | --- |
| Process crash, same boot (OOM, panic, kill) | No loss | No loss: the page cache holds every write |
| Graceful shutdown | No loss | No loss: shutdown `fsync`s every journal |
| One voter's host crashes or loses power | No loss | That voter keeps its verified prefix and rejoins through the recovery gate. The other voters hold every acknowledged write. |
| A majority of voters crash at once | No loss | Writes acknowledged within the unsynced window can be lost. The group stays stopped until an operator accepts that loss. |
| One voter loses its disk | Rejoins empty through the recovery gate | Same |

`never` turns a quorum of simultaneous host crashes from an outage into a
bounded loss of the most recent writes. Choosing it is an explicit operator
decision.

## Metadata and run state

Each core keeps a small metadata file next to its journal. It holds every
group's vote and its log state: `Empty`, `Initialized` (the group has persisted
membership or an entry) or `Recovering` (the replica may be missing entries it
acknowledged). `Recovering` stays recorded until the group's recovery gate
opens, so a process crash or a clean shutdown in the meantime comes back gated.
The file is replaced with a temporary file, `fsync`, rename and directory
`fsync`. Votes change only during elections, so this cost stays off
the data path. The vote no longer forces an `fsync` of the shared journal,
which with `fdatasync` would flush every group's dirty pages on that core.

The node keeps a run-state file with:

- the boot id of the run that last opened the journals;
- the `fsync` policy of that run;
- the run's status: `running`, `clean` or `poisoned`;
- a recovery epoch, raised by every run that needs a verified-prefix read, so
  that a core opened later in the run is still read that way once.

At startup the node reads it, decides how to open the journals, and then
durably records the current boot id with `clean = false`. It does this before
any new journal write. On graceful shutdown it stops the Raft cores, `fsync`s
every journal and only then writes `clean = true`. A real shutdown path
replaces the current `process::exit(0)`.

The boot id comes from `/proc/sys/kernel/random/boot_id`. Inside a container
that is the host's boot id. Where it is unavailable, a missing `clean` flag is
treated as an unclean crash.

## Opening the journal

| Previous run | Interpretation | Journal read | Recovery gate |
| --- | --- | --- | --- |
| No run state, no journal | New node or new disk | None | Bootstrap probe; never initialize a group that a peer reports initialized |
| No run state, a journal holds records | Unknown history | Verified prefix | Every initialized group |
| `clean` | Every write is on disk | Strict | No |
| `running`, same boot id | Process crash; the page cache survived | Strict | No |
| `running`, other or unknown boot id, policy `always` | Host crash; every acknowledged write was fsynced | Verified prefix, because committed and truncate markers are unsynced | No |
| `running`, other or unknown boot id, policy `never` | Host crash; writeback may have left holes | Verified prefix | Every initialized group |
| `poisoned` | An I/O error stopped the previous run | Verified prefix | Every initialized group |

Strict tolerates only an incomplete final frame; any other corruption fails
closed. Verified prefix keeps the frames up to the first one that fails
verification and truncates the rest, and the journal is rewritten before the
core writes again.

Writeback after a host crash can persist later pages before earlier ones.
Verification therefore cannot skip a bad frame. Because frame checksums cover
the header, the payload and the segment sequence number, the kept frames form
a valid log prefix.

## Recovery gate

A replica whose log may be missing entries it acknowledged must not help
elect a leader that lacks them. While gated it does not campaign and it grants
no votes. The gate opens once the replica has applied the committed index
returned by a fresh outbound ReadIndex barrier from the current leader. This
is the barrier from the memory-WAL rejoin work, now applied to any replica in
the recovery state.

The vote is restored from the metadata file before the Raft core starts, so a
recovering replica still rejects appends from a leader with a stale term. A
recovering replica that led its group starts as a follower: OpenRaft restores a
replica whose committed vote names itself as that term's leader without an
election, and with a truncated log it would reuse the log ids of entries it
lost and fork the group.

On the leader, a follower whose log moved backwards is rebuilt through the
existing remove, learner and promote steps. If a majority of the followers
moved backwards while the leader kept its log, removal cannot commit, so the
leader rewinds their replication progress instead. Neither case needs an
operator. In managed mode this becomes the control plane's `RebuildReplica`
operation.

If a majority of a group's voters are gated, no leader can produce a barrier.
A gated replica that applies nothing for 30 seconds reports itself stalled,
and the group stays stopped. An operator then accepts the loss of the unsynced
tail on the replicas with the longest last log id until a majority of the
voters is open, and normal election picks the longest verified log. Accepting
on more replicas than needed lets election choose any log at least as long as
a majority's. This replaces `adopt-survivor` and `reinitialize`. A group whose
state is `Initialized` or `Recovering` never runs `Initialize`, which replaces
the S3 initialized markers and the restart guard.

## Journal hardening

- **Fail-stop.** Any write or `fsync` error stops the core writer. Pending
  requests fail, the writer tries to record `poisoned = true`, and the process
  aborts. It never appends after a partial frame and never retries a failed
  `fsync`.
- **Format epoch 3.** Each frame's checksum covers its length, its payload and
  the segment sequence number. Epoch 2 journals are refused.
- **Segments.** The journal rotates at a target size. Purge deletes whole
  segments once no group needs them. Live entries left in old segments are
  rewritten in bounded chunks, never as one frame per group. Groups that keep
  old segments alive are reported to the snapshot driver. This replaces the
  whole-journal generation rewrite and the 512 MiB frame limit it can hit.
- **Reclaim errors** never fail a batch whose writes are already durable.
- **Bounded memory.** Groups keep an index of entry positions and a bounded
  cache of recent entries. Older entries are read from disk.

## Testing

The journal performs file operations through an I/O trait with two
implementations: the operating system, and a madsim disk. The madsim disk
keeps synced bytes, drops or reorders unsynced pages on a simulated host
crash, keeps every byte on a simulated process crash, and injects I/O errors.
DST then exercises the production journal, and `memory.rs` is deleted.

For each policy, invariants check that no acknowledged write is lost within
the contract above, that gated replicas never vote, and that majority loss
stops the group until an operator accepts it.

## Removed

- The memory log store, `raft.wal.backend`, `allow_volatile_multi_peer`,
  `memory_bootstrap_marker_dir`, and the chart's `storageMode` and
  `allowVolatileMultiPeer`.
- The restart guard and its S3 initialized markers.
- The memory-only rejoin entry points, `adopt-survivor` and `reinitialize`.
- The single-node development mode moves to a disk journal under its data
  directory.

## Delivery

1. The journal I/O trait and the madsim disk, with DST running on the
   production journal (#395).
2. Fail-stop on I/O errors, format epoch 3 and reclaim fixes (#396).
3. The `fsync` policy, the metadata and run-state files and the shutdown path
   (#397), the recovery gate and the default `never` (#398), then removal of
   the memory backend.
4. Segments, rewrite-based reclaim and bounded memory.

The meta-Raft control plane work starts after step 3.
