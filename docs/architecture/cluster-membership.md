# Cluster membership and maintenance

This document describes how Ursula decides which nodes replicate each Raft
group, how a group recovers a replica that lost its log, and how operators and
Kubernetes replace a voter without losing quorum.

## 1. Membership model

`raft.peers` lists every node and its cluster URL. `raft.group_count` fixes
the number of data groups, and streams hash to a group by that count. The meta
group places every data group at its `Bootstrap`, by the seed rule over the
`raft.peers` ids at genesis (`crates/ursula-control/src/placement/seed.rs`):
group `g` runs on `min(raft.placement.replication_factor, N)` consecutive nodes
in ascending id from index `g mod N`, and the first of them initializes it.
With `N` at most the replication factor, every node votes in every group.

Placement decides what each node hosts. The engine factory takes every input
placement decides through one seam, `PlacementInputs`
(`crates/ursula-raft/src/placement_wiring.rs`): which groups the node hosts
and how each may open, the replica's seed role, the voters its recovery
drivers ask and initialize, and the membership target its reconcile shell
pursues (section 2), read at each use. On a server node it is
`MetaPlacement`. The node's view gives the seed role, the voters and the
target, and the hosting set gives the admission. Only the hosting driver writes that
set (`crates/ursula-raft/src/hosting.rs`, started by
`crates/ursula/src/bootstrap/hosting.rs`). The runtime starts with an empty
set, so no replica opens before the driver admits it. The driver starts once
the meta runtime has published its first view. Before, the node opens the
cores its journals hold, in parallel. The driver's first plan then reopens
every group a journal holds, unconfirmed and without a meta leader. It
creates a group only from a view a meta read barrier confirmed, on a node
that view places in the group. No open failure stops the start: such a group
stays closed under its open back-off, and the driver opens it again. A node
that `raft.peers` gained after genesis is in no group's seeds, so it hosts
nothing.

The voters each group's placement gives are also what the node's maintenance
inventory expects, none before a bootstrapped view. A node that hosts no
replica of a group, or whose replica knows no leader and does not count as a
voter, answers `307` (`NotHosted`) toward a node of the first tier of the
group's leader candidates in its latest view (`ursula_control::leader_candidates`):
the voters of both sides of a move, or the current voters, rotating per
answer so that successive answers name different nodes. Without a view the
only tier is every other `raft.peers` node. A replica that counts as a voter
and knows no leader answers `503` with `Retry-After`, since its group has
none. The admin plane never answers `NotHosted`, whose `Location` would name
a client-plane URL: it answers that `503`, and `ursulactl`, which follows no
redirect, asks its next node. The
egress probe judges each voter config of the effective membership of every
replica that counts as a voter, when one of the configs lists the node
([Raft WAL](raft-wal.md#recovery-gate)), and probes each peer once per tick.

`raft.peers` is the only source of peer addresses, and Raft membership is the
electorate. The server builds one `PeerDirectory` from `raft.peers` at config
load (`crates/ursula-raft/src/peer_directory.rs`), lends it at startup (the
format-epoch probe and meta's genesis node set) and moves it into the node's
one bind. Every other reader of a peer
address reads the bound directory at each use (`PeerCluster::directory`): the
Raft network client, leader forwarding, recovery probes, the egress probe, the
leadership-shed polls, the shutdown handoff and leader redirects. The directory
is not `Clone`, so no reader duplicates it, and a reader takes it per use. Membership and recovery code deal in
node IDs only. A node that a group's membership names but `raft.peers` does not
list is never dialed: its network client answers unreachable without dialing,
followers neither forward to it nor redirect to it, and each node logs it once
(`PeerCluster::cluster_url` in `crates/ursula-raft/src/peer_cluster.rs`). The
server binds the directory with the node's identity before its runtime starts
(`crates/ursula/src/bootstrap/meta.rs`). Every node with a WAL path lists
itself in `raft.peers`, a single node included. A node on a temporary WAL
with an empty `raft.peers` runs with an entry of itself, at its cluster
listener, else its client listener (`complete_temporary_node` in
`crates/ursula/src/server.rs`). To move a node, change `raft.peers` on every node and restart them. Membership
entries, snapshot records and installed snapshots name each member by node ID
only: OpenRaft's `Node` is `EmptyNode` in `crates/ursula-raft/src/types.rs`.

The meta Raft group forms over the nodes of `raft.peers` and records the
cluster's bootstrap and every group's placement. Every node that runs Raft
runs a replica of it, unless its meta journal or replica is damaged. The
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
not pass meta from voter to voter. A data group's leader reports the voters
it formed on to the meta group, which makes the group `Live` (section 2).
The admin routes (section 3) can change a group's OpenRaft membership
directly. Nothing changes it back.

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
entries are a majority too. The replica's membership reconcile shell
(`crates/ursula-raft/src/rejoin/reconcile.rs`) observes and counts each
reversion.

Every replica runs that shell. On the leader it reads the group's target
from its placement in the node's latest view: the placement's target voters,
`Seeded` or `Moving` at the placement's key, or `Settled` for a live group
that does not move. It asks the pure planner
(`crates/ursula-control/src/placement/reconcile.rs`) for one step and runs it
under a compare-and-set on the leadership and the membership it was planned
from. A seeded group only reports, and a settled group changes no voter, so
until a move the shell flattens a joint configuration and reports. When the
committed uniform membership has the target's voters under a seeded or
moving target, the leader proves it still leads with a fresh quorum barrier,
reads its vote and committed membership again, and submits
`ReportConverged` with the placement's key, the voters and the membership's
log id. The meta group accepts it only while the placement is still at that
key, so a report from a deposed leader or of an older placement changes
nothing. An accepted report makes the group `Live` and stable on those
voters, certified at that membership. A refused stale report publishes
`stale_view`, and a report that may have applied makes the shell confirm
its view before it plans again. Each group's metrics on `GET
/__ursula/metrics` carry the shell's status under `reconcile`: its state
(`stepping`, `waiting` with its reason, `reporting`, `stale_view` or
`converged`), its last failed step, and the placement and meta entry it
read. The shell asks no learner's recovery gate yet, so it promotes no
learner. A group placed by hand in tests and the simulation
(`FixedPlacement`) has no target, and its shell only observes.

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
`released`. A node registers its root at every start, and says whether meta
never bound that root (`empty`). The root's identity file records how it
stands: `unbound` from its creation until meta first answers a
registration of it, and `bound` from then on. The run records the binding,
synced, before it acts on the answer, so a root whose data replicas took
part is always recorded bound. What the root's journals hold decides
nothing. The first registration of an id binds its root, and a registration
of the bound root changes nothing. A released slot binds only an `empty`
root other than the released one, and the released root never binds again
(`crates/ursula-control/src/placement/apply.rs`). The
placement status shows every slot: the bootstrapped cluster's node records,
and before Bootstrap the `registering` view's map from each registered id to
its slot.

The registration judges a refusal for another root by how the run stands on
its root (`crates/ursula-raft/src/meta/registration.rs`,
`crates/ursula-raft/src/meta/root.rs`). An unbound root never fences
itself, and a restart keeps it unbound whatever its meta replica wrote
before, a vote floor for example. Its data replicas open with the run
but are held out from its open until meta binds the root and then through
the predecessor wait (`crates/ursula-raft/src/predecessor_wait.rs`): they
grant no vote, run no gate driver, write no vote floor and no snapshot
object, so their gates stay closed and they take no replication. Its meta
replica's votes and genesis vote floor are not held before an answer, so a
forming cluster's meta group elects. Its meta barrier rounds are held until
the binding, as the data replicas are: a barrier needs a meta leader, which
answers the registration too, so a new disk under a live meta voter takes no
meta floor before meta refused or bound it, and with no floor its closed
gate grants no meta vote and takes no replication. A fresh cluster's data groups therefore form only once meta
answered its nodes' registrations, and genesis values that disagree, which
keep meta from forming, keep the data groups from forming too. A run of an
unbound root whose meta journal or meta replica does not open gets no
answer, so its data replicas stay held for that whole run. That is safe, and
it is the one meta failure that stops a node from serving data. A bound root
does not wait for meta. A refusal for another root makes the run owe the
predecessor wait:
from then on its meta replica grants no vote and writes no floor, and the
node sheds every leadership. Its registration status is `root_refused` with
the slot, readiness reports `root_refused`, and it registers again at most
5 seconds apart. Once an operator released the old root it binds with no
restart, waits out its predecessor (readiness `predecessor_wait`), and its
replicas rejoin through their gates as in the rest of this section.

A bound root is superseded for good on a registration refused for another
root, on any applied view that
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
| `POST /__ursula/raft/{target}/recovery/accept-unsynced-loss` | Take a recovery plan for a stalled group. Its `floor` names the plane (`meta`, or `seeded` or `certified` for a data group), and a floor of the other plane is refused with `floor_other_plane` before the plan runs. A `certified` floor names the membership entry the group's last convergence certified: the plan's leader refuses unless it holds it (`floor_not_held`), and every replica that holds it keeps it through a cut. |
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
(`check_raft_maintenance` in `crates/ursula-raft/src/maintenance.rs`, schema
version 2). The inventory comes from the node's placement view: the groups in
which its bootstrapped view gives the node a role, each with its role, its
target voters and whether the replica counts as a voter by the one voter
predicate. The voter checks come from each replica's effective membership. A
replica that does not count as a voter, such as an incoming learner, is
checked only for being stopped. That blocks nothing while the report lists
the replica in `expected_groups` or `unplaced`. Before a bootstrapped view
and while meta is damaged the report lists no replica, so a stopped replica
blocks as a voter's would. Node issues come
from the view, the meta replica's status and the hosting driver's: a node
that does not know its groups (`placement_unknown`, a wiped voter before meta
answers), a view that has not bootstrapped, a genesis mismatch, a meta group
without a leader for 30 s, a damaged meta replica, which replaces every issue
the view gives, and a configured group count or replication factor the
cluster does not share. Group issues name a missing placed replica, a replica
held without a role (`unplaced_replica`), an incomplete voter set, a member
without a role (`unplaced_member`: the leader removes such a learner once the
group is seeded or its voters equal the target, and such a voter only by a
move or by leaving a joint configuration; nothing restores a missing voter),
`joint_membership`, `apply_lag` or `apply_stopped`. A node with no group is
ready. `GET /__ursula/ready` uses `serving_ready()`, which ignores incomplete
voter sets, members and replicas without a role, joint membership and stopped
apply, so healthy survivors keep serving while a peer catches up. Maintenance
tooling uses `ready()`, which ignores only a meta group without a leader and a
damaged meta replica, so a second disruption waits for full redundancy. The
readiness body includes the full report.

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
