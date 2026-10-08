# Single Raft WAL with an fsync policy

Status: implemented, shipped in Ursula 0.7.0. It removed the memory WAL
backend and extends the per-core journal described in
[Production Raft WAL](raft-wal-production.md). The text describes 0.7.0,
including the OpenRaft 0.10.0-alpha.28 upgrade (#466) and the handoff checks
(#467).

## Decision

- Ursula has one Raft WAL backend: the per-core shared journal. The memory
  backend is removed. Configurations that select it fail to load, and 0.7.0
  has no migration path from 0.6.
- `raft.wal.fsync` chooses when journal appends reach stable storage:
  - `never` (default): ordinary data appends are written to the page cache and
    acknowledged without `fsync`. Batches containing a Raft membership entry
    are always synced before acknowledgement. This preserves the voter set
    needed to recover after a full-cluster power loss, even when unsynced data
    loss is accepted.
  - `always`: every batch that carries entries or a purge is acknowledged
    only after `fsync`, as the 0.6 disk WAL did. Commit and truncate markers
    are not synced on their own.

  There is no interval mode. After a host crash under `never` a replica keeps
  only its verified prefix and rejoins through the recovery gate, so a
  periodic `fsync` would not change what recovery does.
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
| One voter's host crashes or loses power | No loss. If the crash leaves a complete invalid frame in a core's newest segment, that core's groups rejoin through the recovery gate. | That voter keeps its verified prefix and rejoins through the recovery gate. The other voters hold every acknowledged write. |
| A majority of voters crash at once | No loss. If the crashes leave a complete invalid frame in the newest segment on a majority of a group's voters, the group stays stopped until an operator accepts a loss, although none happened. | Writes acknowledged within the unsynced window can be lost. The group stays stopped until an operator accepts that loss. |
| One voter loses its disk | Rejoins empty through the recovery gate | Same |

`never` turns a quorum of simultaneous host crashes from an outage into a
bounded loss of the most recent writes. Choosing it is an explicit operator
decision.

`always` can still need the operator call. The newest segment can still hold
bytes written after the last `fsync`: an unacknowledged batch, or a commit or
truncate marker. A host crash can leave a complete frame there that fails
verification, such as a hole, a zero-filled tail or a torn sector inside an
unacknowledged batch. Recovery cannot tell such a cut from media corruption of
an acknowledged write, so it gates every group on that core before it cuts the
frame. That is the safe choice, and it is deliberate. A group whose other
voters are healthy rejoins without an operator. A single-voter group, or a
group with a majority of its voters gated together, for example by a rack
power loss, stays stopped until an operator calls
`POST /__ursula/raft/{group}/recovery/accept-unsynced-loss`, although no
acknowledged write was lost.

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
durably records the current boot id with status `running`. It does this before
any new journal write. On graceful shutdown it stops the Raft cores, `fsync`s
every journal and only then records status `clean`. Shutdown has a deadline of
20 seconds from the first signal. A second signal, or a shutdown still running
at the deadline, exits at once without recording `clean`, so the next start
treats the run as a crash.

The boot id comes from `/proc/sys/kernel/random/boot_id`. Inside a container
that is the host's boot id. Where it is unavailable, a run that did not
record `clean` is read as a host crash under its recorded policy.

## Opening the journal

| Previous run | Interpretation | Journal read | Recovery gate |
| --- | --- | --- | --- |
| No run state, no journal | New node or new disk | None | Bootstrap probe; never initialize a group that a peer reports initialized |
| No run state, a journal holds records | Unknown history | Strict sealed segments, verified newest tail, kept segments rewritten | Every initialized group |
| `clean` | Every write was synced | Strict sealed segments, verified newest tail | Only if a complete invalid frame is cut |
| `running`, same boot id | Process crash; the page cache survived | Strict sealed segments, verified newest tail | Only if a complete invalid frame is cut |
| `running`, other or unknown boot id, policy `always` | Host crash with every acknowledged write fsynced | Strict sealed segments, verified newest tail | Initialized groups if a complete invalid frame is cut |
| `running`, other or unknown boot id, policy `never` | Host crash; writeback may have left holes | Strict sealed segments, verified newest tail, kept segments rewritten | Every initialized group |
| `poisoned` | An I/O error stopped the previous run | Strict sealed segments, verified newest tail, kept segments rewritten | Every initialized group |

Sealed segments are always read strictly, whatever the previous run was.
Invalid or incomplete sealed frames and missing segment sequences fail startup
without changing the journal. The newest segment keeps only the prefix before
the first frame that fails verification. Before cutting a complete invalid
frame, recovery durably closes the core's recovery gates. This rule also
applies after a clean or process restart, so another crash during startup
cannot change the repair policy. After a run that may have lost writes, the
kept segments are also written again and `fsync`ed, because a failed `fsync`
can leave frames that only the page cache holds.

Recovery uses the previous run's recorded policy, even when configuration has
changed. The newest segment may contain an unacknowledged
write or an unsynced commit or truncate marker. Recovery keeps its verified
prefix and records the core's groups as recovering before cutting a complete
invalid frame. An incomplete newest frame remains recoverable without gating healthy
groups.

Writeback after a host crash can persist later pages before earlier ones.
Verification therefore cannot skip a bad frame. Because frame checksums cover
the header, the payload and the segment sequence number, the kept frames form
a valid log prefix.

The run state records the current run's policy, and a later host crash is
read by it, so that policy must hold for every write the journals hold. A
process crash under `never` leaves acknowledged writes in the page cache only.
A run that starts with `always` after such a crash therefore `fsync`s the
newest segment of every core journal and its directory before it records
itself. Older segments were `fsync`ed when they were sealed. If the host
crashes before that record, the run state still names the `never` run and the
next start gates every group. A run that keeps `never` has nothing to sync,
since a host crash after it gates every group anyway.

## Recovery gate

A replica whose log may be missing entries it acknowledged must not help
elect a leader that lacks them. While gated it does not campaign and takes no
leadership transfer. It grants no vote once it knows the group holds entries,
so an empty replica still votes in a new group's first election. The gate
opens once the replica has applied the committed index returned by a fresh
outbound ReadIndex barrier from the current leader. This is the barrier from
the memory-WAL rejoin work, now applied to any replica in the recovery state.

The vote is restored from the metadata file before the Raft core starts, so a
recovering replica still rejects appends from a leader with a stale term. A
replica that led its group starts as a follower after any restart that was not
clean, and whenever it is recovering: OpenRaft restores a
replica whose committed vote names itself as that term's leader without an
election, and with a truncated log it would reuse the log ids of entries it
lost and fork the group. Before its Raft core starts, the replica records its
vote for itself uncommitted in the metadata file, with the same `fsync`ed
write as any vote, and only then runs with it. A failed write stops the group
from starting. The committed vote is then gone from disk, so no later start
restores the leadership, whether it follows a clean shutdown or comes after
the gate opened, by a barrier or by an operator. A vote for another replica is
never changed, and nothing is written for it.

On the leader, a follower whose log moved backwards is rebuilt through the
remove, learner and promote steps. The leader removes it only when a quorum of
voters kept their log and acknowledged the leader within one minimum election
timeout (1.5 seconds), counting the leader itself. Otherwise the removal might
not commit, so the leader rewinds the replication progress of the voters that
lost entries instead. Neither case needs an operator.

A leader hands its leadership only to another voter that kept its log,
acknowledged the leader within one minimum election timeout and holds every
committed entry. Every leadership planner and the admin transfer endpoint use
this check. If the target has not taken over within two maximum election
timeouts (6 seconds), the leader steps down and campaigns again.

If a majority of a group's voters are gated, no leader can produce a barrier.
A gated replica that applies nothing for 30 seconds reports itself stalled:
readiness answers `503` with reason `recovery_stalled` and lists the group. The
group stays stopped. An operator then accepts the loss of the unsynced tail on
the replicas with the longest last log id until a majority of the voters is
open, and normal election picks the longest verified log. Metrics show each
replica's last log index but not its term, so the operator runbook ranks by
index. Accepting
on more replicas than needed lets election choose any log at least as long as
a majority's. This replaces `adopt-survivor` and `reinitialize`. A group whose
state is `Initialized` or `Recovering` never runs `Initialize`, which replaces
the S3 initialized markers and the restart guard.

An acceptance is a compare-and-act on what the operator saw. Its request names
the replica's last log index and current term, as the group's metrics showed
them. The replica refuses it with `409 Conflict`, and changes nothing, unless
it has a durable vote floor, its gate is stalled and it still holds that log. A
replica whose gate is already open answers `200` with outcome `already_open`.
Accepting unsynced data loss cannot replace lost voting history. A replica
without a floor must obtain a fresh quorum proof before it can accept
replication or open its gate. A gate that awaits or
applies a barrier may still open without losing anything, and a replica whose
log moved since the operator looked is no longer the one the operator chose.
The admin incarnation precondition still applies, so a restarted process
refuses a plan made against the one before it.

### Lost vote history

A replica with a missing vote rejects appends, heartbeats and snapshots until
it establishes a durable vote floor. It asks a reachable leader for the existing
ReadIndex barrier. Since the replica cannot acknowledge replication yet, that
proof requires a current quorum without it. The proven vote passes through
OpenRaft and is persisted before replication is admitted. Missing `journal.meta`
beside surviving records is also treated as lost vote history.

This extra admission step applies only to replicas that lost their vote. A
recovering replica with a retained vote can receive replication immediately.
Initial cluster bootstrap separately waits for every configured peer to confirm
empty history and persist its initial floor, the vote `(0, 0)`. That floor
survives a restart like any durable vote: granting a vote or acknowledging a
leader persists a higher vote first, so a durable `(0, 0)` proves that neither
happened. A peer that restarts between the initializer's `Initialize` and its
first vote therefore still votes in the group's first election.

The regression `a_wiped_voter_never_lets_a_stale_leader_commit` reproduces the
lost vote with a clean restart of the old leader, which still leads with its
committed vote, and rejects conflicting entries at the same committed index.
Its process-crash half checks only that the crashed old leader restarts
demoted, with an uncommitted vote, before it writes. That demotion alone keeps
the old leader from committing, so the crash half does not exercise the vote
floor.

## Journal hardening

- **Fail-stop.** Any write or `fsync` error stops the core writer. Pending
  requests fail, the writer tries to record the status `poisoned`, and the process
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
stops the group until an operator accepts it. Regression scenarios check that
an accepted leader that restarts never forks the log, that a switch from
`never` to `always` across a process crash keeps every acknowledged write or
gates the group, and that an acceptance opens only a stalled gate for the log
the operator saw.

## Removed

- The memory log store, `raft.wal.backend`, `raft.wal.allow_volatile_multi_peer`,
  `raft.memory_bootstrap_marker_dir`, `raft.rejoin_probe`, and the chart's
  `raft.storageMode`, `raft.allowVolatileMultiPeer` and `persistence.enabled`.
- The restart guard and its S3 initialized markers.
- The memory-only rejoin entry points, `adopt-survivor` and `reinitialize`.
- Zero-config development mode (the `default` preset on one node) runs
  without Raft. Any other single Raft node without `raft.wal.path` keeps its
  journal in a temporary directory that a clean shutdown removes.

## Delivery

1. The journal I/O trait and the madsim disk, with DST running on the
   production journal (#395).
2. Fail-stop on I/O errors, format epoch 3 and reclaim fixes (#396).
3. The `fsync` policy, the metadata and run-state files and the shutdown path
   (#397), the recovery gate and the default `never` (#398), then removal of
   the memory backend (#399). Deterministic replay of journal simulations
   (#400) followed.
4. Segments, rewrite-based reclaim and bounded memory (#401).

Dynamic membership is tracked in
[Dynamic Group Membership](dynamic-group-membership.md).
