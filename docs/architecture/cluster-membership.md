# Cluster membership and maintenance

This document describes how Ursula decides which nodes replicate each Raft
group, how a group recovers a replica that lost its log, and how operators and
Kubernetes replace a voter without losing quorum.

## 1. Membership model

Membership is static and comes from configuration. `raft.peers` lists every
node and its cluster URL. `raft.group_count` fixes the number of data groups,
and streams hash to a group by that count. Each data group's voters and its
initializer come from the seed rule over the `raft.peers` ids
(`crates/ursula-control/src/placement/seed.rs`): group `g` runs on
`min(raft.placement.replication_factor, N)` consecutive nodes in ascending id
from index `g mod N`, and the first of them initializes it. With `N` at most the
replication factor, every node votes in every group. `ConfigSeeds`
(`crates/ursula-raft/src/placement_wiring.rs`) computes this table, and the
server reads it for what a node hosts, where it routes a group it does not host,
its maintenance inventory and its egress probe. The engine factory takes every
input placement decides through one seam, `PlacementInputs`: which groups the
node hosts and how each may open, the replica's seed role, and the voters its
recovery drivers ask, initialize and heal toward, read at each use. The meta group's `Bootstrap` seeds the same rule
over the `raft.peers` ids at genesis, so the two agree only while `raft.peers`
keeps those ids: do not add nodes to `raft.peers` of a running cluster.

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

There is no runtime membership authority for data groups. The meta Raft
group forms over the nodes of `raft.peers` and records the cluster's
bootstrap. Every node that runs Raft runs a replica of it, unless its meta
journal or replica is damaged. No data group reads its placements, and the
server never moves a group to another node or changes its voter count.
`GET /__ursula/placement`, on the client and the admin listener, reports what
the node's meta replica applied, its role and leader, and its registration
(`crates/ursula-raft/src/meta/status.rs`). With `?confirmed=true`, on the
admin listener only, a meta read barrier confirms the view first. A node that
runs no meta replica marks the `placement-unavailable` leadership shed. It
still campaigns, so a group whose every voter lacks meta still elects, but it
takes no leadership transfer and hands the leaderships it holds to its peers,
again every 30 seconds (`crates/ursula/src/bootstrap/meta_shed.rs`). A node
that sheds for a reason that refuses transfers, such as the maintenance
drain, hands its meta leadership away too. A node that sheds only for cold
health keeps it, so an object store outage that every node sheds for does
not pass meta from voter to voter. The configured voter set is the target that the heal driver restores
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

A group's seed initializer runs `Initialize` only when every seed voter
reports an empty log, no leader and a persisted genesis floor, and it confirms
its seed role right before it does. A replica writes its genesis floor only
once it confirmed a seed role. A replica that
ever held the group never initializes it again.

### Roots in meta and disk replacement

Each node id's record in meta holds a root slot: the root (the WAL disk,
named by the id `topology.bin` drew) the id is bound to, `bound` or
`released`. A node registers its root at every start, and says whether the
root was empty when it opened. The first registration of an id binds its
root, and a registration of the bound root changes nothing. A released slot
binds only an empty root other than the released one, and the released root
never binds again (`crates/ursula-control/src/placement/apply.rs`). The
placement status shows every slot: the bootstrapped cluster's node records,
and before Bootstrap the `registering` view's map from each registered id to
its slot.

The registration judges a refusal for another root by how the run stands on
its root (`crates/ursula-raft/src/meta/registration.rs`,
`crates/ursula-raft/src/meta/root.rs`). An empty root that meta has not
bound in this run never fences itself. Its data replicas open with the run
but are held out from its open until meta binds the root and then through
the predecessor wait (`crates/ursula-raft/src/predecessor_wait.rs`): they
grant no vote, run no gate driver, write no vote floor and no snapshot
object, so their gates stay closed and they take no replication. Its meta
replica is not held before an answer, so a forming cluster's meta group
elects. A refusal for another root makes the run owe the predecessor wait:
from then on its meta replica grants no vote and writes no floor, and the
node sheds every leadership. Its registration status is `root_refused` with
the slot, readiness reports `root_refused`, and it registers again at most
5 seconds apart. Once an operator released the old root it binds with no
restart, waits out its predecessor (readiness `predecessor_wait`), and its
replicas rejoin through their gates as in the rest of this section.

A root that holds state, or that meta bound in this run, is superseded for
good on a registration refused for another root, on any applied view that
shows its root released, and on a view a meta read barrier confirmed that
binds its id to another root. An unconfirmed view may show the slot of a
replica still applying history, so only a release counts there. A
superseded process takes part in no Raft traffic, writes nothing more to
its WAL, never records a clean end, answers clients `503` and reports
readiness `superseded` until it exits.

`ursulactl node replace-disk --node N --expect-root R` releases `R`
through `POST /__ursula/placement/nodes/{node}/replace-disk` on another
node's admin plane, a compare-and-set on the expected root that answers the
node's record and `409` (`stale_root`) for a slot on another root. It reads
`N`'s slot from a confirmed view first. Without `--force` it refuses when
`N`'s own process still runs on `R` or reports ready. A release is safe only
once the old disk is revoked: the old process ended, its volume was detached
or destroyed, or its host is shut down. A process without its disk stops
within its disk lease, and the new root waits longer than any lease from its
binding, so the old process falls silent first.

## 3. Admin membership surface

The admin plane binds `server.admin_listen`, which is pod loopback by default
(`admin_router` in `crates/ursula/src/lib.rs`). Its per-group routes name a
`{target}`, a data group's id or `meta`, the meta group
(`parse_raft_target`). The membership and purge routes serve data groups only
and answer `400` for `meta`:

| Route | Purpose |
| --- | --- |
| `POST /__ursula/raft/{target}/membership?voters=` | Change the voter set. Only the leader accepts it, others answer `409`. |
| `POST /__ursula/raft/{target}/leader/transfer/{node}` | Transfer leadership. |
| `GET /__ursula/raft/{target}/quorum` | Confirm a fresh quorum prefix for this group. |
| `POST /__ursula/raft/{target}/self-election` | Start an election on this replica while its term is unchanged. |
| `POST /__ursula/raft/{target}/recovery/accept-unsynced-loss` | Take a recovery plan for a stalled group. Its `floor` names the plane (`meta` or `seeded`), and a floor of the other plane is refused with `floor_other_plane` before the plan runs. |
| `POST /__ursula/raft/{target}/snapshot`, `.../purge` | Build a snapshot or purge the log. |
| `POST`/`DELETE /__ursula/leadership-shed/maintenance` | Set or clear the maintenance-drain mark. |
| `POST /__ursula/placement/nodes/{node}/replace-disk` | Release the root the node id is bound to, given as `{"expect_root": R}` (section 2). |

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
