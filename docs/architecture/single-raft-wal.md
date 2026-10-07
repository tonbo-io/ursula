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
decision. Membership entries are always fsynced, including under `never`, so a
new group retains its election configuration after an immediate power loss.

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
| `running`, other or unknown boot id, policy `always` | Host crash; every acknowledged write was fsynced | Verified prefix, because committed and truncate markers are unsynced | If replay discards bytes, every group on that core |
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
elect a leader that lacks them. While gated it does not campaign and it grants
no votes. The gate opens once the replica has applied the committed index
returned by a fresh outbound ReadIndex barrier from the current leader. This
is the barrier from the memory-WAL rejoin work, now applied to any replica in
the recovery state. The barrier request also fences outstanding append replies
on the leader: replies to RPCs started before this request cannot subsequently
count toward commit. This requires the updated recovery transport on the leader.

Before a gated replica acknowledges replication, it persists a vote floor
under a membership authority. With meta enabled, fresh linearizable meta reads
before and after peer vote samples must agree on the placement, operation and
process identities. The samples must intersect every possible quorum of the
current, previous, desired and temporary survivor configurations. Empty
term-zero peers do not count as historical-vote witnesses. This allows
one surviving data leader to repair followers even when data ReadIndex cannot
yet succeed. A merely local committed-state watch is not sufficient authority.

Without meta, explicitly immutable static cohorts may use their fixed voter
intersection. That mode rejects membership-changing operations; its background
repair driver only rewinds logs and appends a `ReplicationBarrier` no-op, never
removes, promotes or re-adds voters. Unknown mutable membership instead requires
a fresh data-leader ReadIndex proof. Discovery includes live Raft membership
and control-plane addresses; old bootstrap addresses alone are not authority.
A barrier arriving before its vote floor is retried for the same leader.

Fresh genesis is the narrow exception: a replica with no initialized history
can adopt a floor only after every configured peer reports an empty group,
so the first election can precede the first ReadIndex. The meta-controlled
factory separately disables static reinitialization of existing groups.
The `ReplicationBarrier` command is appended to the command enum; old nodes
cannot decode it, so this version requires the documented all-stop upgrade,
not a mixed-version rolling deployment.
Missing metadata beside a nonempty journal is unknown history and enters the
same gate. A missing node run state starts an epoch above every surviving
core's verified epoch.

The vote is restored from the metadata file before the Raft core starts. A
recovering replica that led its group starts as a follower: OpenRaft restores a
replica whose committed vote names itself as that term's leader without an
election, and with a truncated log it would reuse the log ids of entries it
lost and fork the group. Before its Raft core starts, the replica records its
vote for itself uncommitted in the metadata file, with the same `fsync`ed
write as any vote, and only then runs with it. A failed write stops the group
from starting. The committed vote is then gone from disk, so no later start
restores the leadership, whether it follows a clean shutdown or comes after
the gate opened, by a barrier or by an operator. A vote for another replica is
never changed, and nothing is written for it.

On a surviving leader, a follower whose log moved backwards is repaired by
rewinding its replication progress and sending the leader's log again. An idle
group gets a state-preserving `ReplicationBarrier` entry. Background repair
never changes membership, including while a joint configuration is pending.
Managed membership changes require the control plane's typed operation.

If a majority is gated and no surviving leader can repair replication under
authoritative membership, no leader can produce an opening barrier.
A gated replica that applies nothing for 30 seconds reports itself stalled,
and the group stays stopped. An operator then accepts the loss of the unsynced
tail on the replicas with the longest last log id until a majority of the
voters is open, and normal election picks the longest verified log. Accepting
on more replicas than needed lets election choose any log at least as long as
a majority's. This replaces `adopt-survivor` and `reinitialize`. A group whose
state is `Initialized` or `Recovering` never runs `Initialize`, which replaces
the S3 initialized markers and the restart guard. Do not reset votes or rerun
`Initialize` on one replica to recover a missing configuration. Existing media
damage that has destroyed every membership requires an offline restore of the
whole group from a known consistent backup, or explicit recreation as a new
group after retiring the old group on every voter. Accepting an unsynced tail
alone does not authorize discarding durable membership or stream history.

An acceptance is a compare-and-act on what the operator saw. Its request names
the replica's last log index and current term, as the group's metrics showed
them. The replica refuses it with `409 Conflict`, and changes nothing, unless
its gate is stalled and it still holds that log. A gate that awaits or
applies a barrier may still open without losing anything, and a replica whose
log moved since the operator looked is no longer the one the operator chose.
The admin incarnation precondition still applies, so a restarted process
refuses a plan made against the one before it.

### Wiped voters reject stale leaders

A replica that has lost its vote history refuses append and snapshot
acknowledgements until it persists an authority-validated vote floor. It then
rejects lower-term leaders while the recovery gate continues to block elections
until fresh ReadIndex and applied-prefix evidence agree. This closes the stale
leader commit path described in [#405](https://github.com/tonbo-io/ursula/issues/405).
The deterministic wiped-voter regression asserts that an index cannot be
committed with two different entries.

## Journal hardening

- **Fail-stop.** Any write or `fsync` error stops the core writer. Pending
  requests fail, the writer tries to record `poisoned = true`, and the process
  aborts. It never appends after a partial frame and never retries a failed
  `fsync`. If writing the poison marker also fails, a same-boot restart still
  uses strict checksum/sequence replay: malformed frames fail startup; intact
  page-cache frames preserve the acknowledged prefix. After a host crash,
  prefix replay that drops any bytes durably gates every group on that core,
  even under `always` (which cannot rule out media corruption).
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
   the memory backend (#399). Deterministic replay of journal simulations
   (#400) followed.
4. Segments, rewrite-based reclaim and bounded memory (#401).

The meta-Raft control plane work starts after step 3.
