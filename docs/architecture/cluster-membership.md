# Cluster membership and maintenance

This document describes how Ursula decides which nodes replicate each Raft
group, how a group recovers a replica that lost its log, and how operators and
Kubernetes replace a voter without losing quorum.

## 1. Membership model

Membership is static and comes from configuration. `raft.peers` lists every
node and its cluster URL. `raft.group_count` fixes the number of data groups,
and streams hash to a group by that count. By default every peer is a voter of
every group. `raft.groups` may instead assign each group a non-empty subset of
the peers, and then it must cover every group in `0..group_count`
(`crates/ursula-config/src/validate.rs`).

`raft.peers` is the only source of peer addresses, and Raft membership is the
electorate. Every reader of a peer address takes it from one `PeerDirectory`
built from `raft.peers` (`crates/ursula-raft/src/peer_directory.rs`): the Raft
network client, leader forwarding, recovery probes, the egress probe, the
leadership-shed polls and leader redirects. Membership and recovery code deal in
node IDs only. A node that a group's membership names but `raft.peers` does not
list is never dialed: its network client answers unreachable without dialing,
followers neither forward to it nor redirect to it, and each node logs it once
(`PeerCluster::cluster_url` in `crates/ursula-raft/src/peer_cluster.rs`). The
server binds the directory with the node's identity before its runtime starts
(`crates/ursula/src/bootstrap/meta.rs`). A node whose `raft.peers` is empty
binds itself alone, at its cluster listener. To move a node, change `raft.peers` on every node and restart them. Membership
entries, snapshot records and installed snapshots name each member by node ID
only: OpenRaft's `Node` is `EmptyNode` in `crates/ursula-raft/src/types.rs`.

There is no runtime membership authority for data groups. Every node that
runs Raft runs a replica of the meta Raft group, unless its meta journal or
replica is damaged, which forms over the nodes of `raft.peers` and records the
cluster's bootstrap, but no data group reads its placements, and the server
never moves a group to another node or changes its voter count.
`GET /__ursula/placement`, on the client and the admin listener, reports what
the node's meta replica applied, its role and leader, and its registration
(`crates/ursula-raft/src/meta/status.rs`). With `?confirmed=true`, on the
admin listener only, a meta read barrier confirms the view first. A node that
runs no meta replica marks the `placement-unavailable` leadership shed. It
still campaigns, so a group whose every voter lacks meta still elects, but it
takes no leadership transfer and hands the leaderships it holds to its peers,
again every 30 seconds (`crates/ursula/src/bootstrap/meta_shed.rs`). A node
that sheds for any reason hands its meta leadership away too. The
configured voter set is the target that the heal driver restores
(section 2). The admin routes (section 3) can change a group's OpenRaft
membership directly, and the heal driver adds a configured voter that is
missing back as a learner and promotes it.

## 2. Replica loss and rejoin

A replica can lose acknowledged entries in two ways. Its host crashes under
`raft.wal.fsync = never`, or a journal I/O failure poisons it, and it keeps
only the verified prefix of its journal. Or its journal is wiped or replaced,
for example by a new volume. The same mechanism covers both
(`crates/ursula-raft/src/rejoin.rs`). [Raft WAL](raft-wal.md#recovery-gate) describes how the
journal records the `Recovering` and `Empty` log states that close the gate.

While the gate is closed, the replica does not campaign or take a leadership
transfer, starts as a follower even if it led the group, and refuses votes once
it knows the group holds entries. It still accepts appends from a leader whose
vote is not lower than its persisted vote. It opens the gate after it applies a
fresh ReadIndex barrier from the current leader, and it records the open gate
before it votes again.

The leader repairs the log in place. When a follower answers `Conflict` at or
below an index it had already matched in this leadership, the follower lost
entries. Every group with a recovery gate runs OpenRaft with log reversion
allowed, so the leader rewinds its progress for that follower on the first
such conflict and replicates its log to it again
([Raft WAL](raft-wal.md#recovery-gate)). The follower stays a voter and no
membership changes. Its conflict counts as its acknowledgement of the leader,
so the leader keeps its quorum, and this holds when the followers that lost
entries are a majority too. The heal driver
(`crates/ursula-raft/src/rejoin/heal.rs`) observes and counts each reversion.
It also adds a configured voter that is missing as a learner, promotes it once
it caught up, and flattens a joint configuration whose second step never
committed.

A replica that lost its vote history, such as one on a wiped disk, answers
errors until a quorum without it proves a vote. Two such replicas of three
therefore never heal on their own. If the leader is among the replicas that
lost entries and a majority of a group's voters are gated, no leader can be
elected. The group refuses writes and readiness reports `recovery_stalled`. An
operator who accepts the loss of the unsynced tail calls
`accept-unsynced-loss` on the gated replicas with the longest logs until a
leader is elected.

A group's initializer runs `Initialize` only when every configured voter
reports an empty log, no leader and a persisted genesis floor. A replica that
ever held the group never initializes it again.

## 3. Admin membership surface

The admin plane binds `server.admin_listen`, which is pod loopback by default
(`admin_router` in `crates/ursula/src/lib.rs`). Its per-group routes are:

| Route | Purpose |
| --- | --- |
| `POST /__ursula/raft/{group}/membership?voters=` | Change the voter set. Only the leader accepts it, others answer `409`. |
| `POST /__ursula/raft/{group}/leader/transfer/{node}` | Transfer leadership. |
| `GET /__ursula/raft/{group}/quorum` | Confirm a fresh quorum prefix for this group. |
| `POST /__ursula/raft/{group}/self-election` | Start an election on this replica while its term is unchanged. |
| `POST /__ursula/raft/{group}/recovery/accept-unsynced-loss` | Open a stalled recovery gate for the log the operator observed. |
| `POST /__ursula/raft/{group}/snapshot`, `.../purge` | Build a snapshot or purge the log. |
| `POST`/`DELETE /__ursula/leadership-shed/maintenance` | Set or clear the maintenance-drain mark. |

Every server generates a fresh process incarnation at startup and publishes
it on all listeners. Every mutating admin request, and the quorum proof, must
carry it in `x-ursula-process-incarnation`. A missing header answers
`428 Precondition Required` and a different incarnation answers
`412 Precondition Failed`. A maintenance plan therefore stops when its target
process restarts instead of acting on a new process it never observed.
`ursulactl` remembers the incarnation it first observed for each node and
sends it with every mutation of the same command.

The maintenance-drain mark makes a node shed its leaderships, stop
campaigning and refuse leadership transfers. `ursulactl drain` sets it and
waits until the node leads nothing, the meta group included, and `undrain`
clears it. The node hands its meta leadership away itself, and `ursulactl`
reads it from the `meta` field of the node's metrics, apart from
`raft_groups`. The mark lives in the process, so a restarted server starts
without it.

Readiness and maintenance eligibility are two answers from one report
(`check_raft_maintenance` in `crates/ursula-raft/src/maintenance.rs`). The
report compares local replicas against the configured inventory and lists
issues per group, such as `incomplete_voter_set`, `unconfigured_member` (a
member that is not a configured voter, which the leader never removes),
`joint_membership`, `apply_lag` or `apply_stopped`. `GET /__ursula/ready` uses
`serving_ready()`, which ignores incomplete voter sets, unconfigured members,
joint membership and stopped apply, so
healthy survivors keep serving while a peer catches up. Maintenance tooling uses
`ready()`, which requires no issue at all, so a second disruption waits for
full redundancy. The readiness body includes the full report.

## 4. Kubernetes integration

The chart's StatefulSet always updates by `RollingUpdate` under the `Parallel`
pod management policy. The controller replaces one pod at a time, from the
highest ordinal down, and moves on only once the replaced pod passes its
readiness probe (`GET /__ursula/ready`), which the chart always renders. A
replaced voter runs the graceful shutdown on `SIGTERM`: it hands off its
leaderships, then records a clean shutdown, so its restart reads its journals
strictly. The controller does not look at the other pods, so an upgrade starts
only once `ursulactl verify-cluster` passes, and the default
PodDisruptionBudget limits evictions to one voter at a time.

`Parallel` matters for a listener move (`server.clusterListen`,
`server.ports.cluster`). Every server pod is deleted at once after the
`helm upgrade`, and the controller recreates them together. Under
`OrderedReady` it would recreate one at a time, each waiting for a readiness
that needs the others.
