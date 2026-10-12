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

The supported production topology is three voters, node IDs `{1, 2, 3}`, on
every group. Startup ownership refuses any other topology
(`validate_startup_topology` in `crates/ursula/src/server.rs`), the shared
reservation accepts only that voter set (`Reservation::validate` in
`crates/ursula-ctl/src/reservation.rs`), and the chart requires
`server.replicaCount = 3` for both.

There is no runtime membership authority. No meta Raft group runs, and the
server never moves a group to another node or changes its voter count. The
configured voter set is the target that the self-heal driver restores
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

The leader repairs the log (`crates/ursula-raft/src/rejoin/heal.rs`). When a
follower answers `Conflict` at or below an index it had already matched in this
leadership, the follower lost entries. OpenRaft never rewinds that progress, so
the network layer reports an error instead, and the leader rebuilds the
follower one membership step at a time: remove the voter, add it back as a
learner, wait for catch-up, promote it. Each step is read off the current membership, so a leader change or a second
restart in the middle converges. The same driver adds a configured voter that
is missing as a learner, removes a learner that lost its log, and flattens a
joint configuration whose second step never committed. When the voters that
lost entries are a majority, no removal can commit. The leader holds every
committed entry, so it rewinds their replication instead and sends them its
log. An idle group proposes an unchanged membership entry to carry the rewind.

If a majority of a group's voters are gated, no leader can confirm a barrier.
The group refuses writes and readiness reports `recovery_stalled`. An operator
who accepts the loss of the unsynced tail calls `accept-unsynced-loss` on the
gated replicas with the longest logs until a leader is elected.

A group's initializer runs `Initialize` only when every configured voter
reports an empty log, no leader and a persisted genesis floor. A replica that
ever held the group never initializes it again.

## 3. Admin membership surface

The admin plane binds `server.admin_listen`, which is pod loopback by default
(`admin_router` in `crates/ursula/src/lib.rs`). Its per-group routes are:

| Route | Purpose |
| --- | --- |
| `POST /__ursula/raft/{group}/membership?voters=` | Change the voter set. Only the leader accepts it, others answer `409`. |
| `POST /__ursula/raft/{group}/learners/{node}?addr=` | Add a learner. |
| `POST /__ursula/raft/{group}/leader/transfer/{node}` | Transfer leadership. |
| `GET /__ursula/raft/{group}/quorum` | Confirm a fresh quorum prefix for this group. |
| `POST /__ursula/raft/{group}/self-election` | Start an election on this replica while its term is unchanged. |
| `POST /__ursula/raft/{group}/recovery/accept-unsynced-loss` | Open a stalled recovery gate for the log the operator observed. |
| `POST /__ursula/raft/{group}/snapshot`, `.../purge` | Build a snapshot or purge the log. |
| `POST`/`DELETE /__ursula/leadership-shed/maintenance` | Set or clear the maintenance-drain mark. |
| `POST /__ursula/maintenance/fence/activate`, `.../retire` | Activate or retire a maintenance executor (section 4). |

Every server generates a fresh process incarnation at startup and publishes
it on all listeners. Every mutating admin request, and the quorum proof, must
carry it in `x-ursula-process-incarnation`. A missing header answers
`428 Precondition Required` and a different incarnation answers
`412 Precondition Failed`. A maintenance plan therefore stops when its target
process restarts instead of acting on a new process it never observed.
`ursulactl pin-incarnations` records the incarnations into the plan that later
verbs use.

The maintenance-drain mark makes a node shed its leaderships, stop
campaigning and refuse leadership transfers. `ursulactl drain` sets it and
waits until the node leads nothing, and `undrain` clears it. A server started
with `URSULA_START_MAINTENANCE_DRAINED=true` starts with the mark set.

Readiness and maintenance eligibility are two answers from one report
(`check_raft_maintenance` in `crates/ursula-raft/src/maintenance.rs`). The
report compares local replicas against the configured inventory and lists
issues per group, such as `incomplete_voter_set`, `joint_membership`,
`apply_lag` or `apply_stopped`. `GET /__ursula/ready` uses `serving_ready()`,
which ignores incomplete voter sets, joint membership and stopped apply, so
healthy survivors keep serving while a peer rebuilds. Maintenance tooling uses
`ready()`, which requires no issue at all, so a second disruption waits for
full redundancy. The readiness body includes the full report.

## 4. Maintenance fence and executor admission

A maintenance fence names one executor of one reservation:
`{reservation_id, executor_id, generation}`. Each server holds one fence state
(`MaintenanceFenceState` in `crates/ursula-proto/src/admin.rs`, enforced by
`crates/ursula/src/admin_fence.rs`):

| State | Mutating admin requests |
| --- | --- |
| `unclaimed` | Admitted without a fence header. This is the default when startup ownership is off. |
| `awaiting_reservation` | Refused. Startup ownership is installed and no reservation has completed. |
| `activating`, `retiring` | Refused until the transition finishes. |
| `active` | Admitted only with `x-ursula-maintenance-fence` equal to the active fence. |
| `retired` | Refused. A retired fence never reopens. |

Activation records the new fence before it drains the local admin command
queue, and it accepts only a generation higher than the current one, or a
retry of the same pending fence. Retirement accepts only the current fence.
An admitted mutation holds a read guard until it completes, even if its HTTP
request is dropped. A mutation that ends abnormally latches the process
uncertain, and every later mutation and transition answers `503` until the
process is replaced. `ursulactl activate-maintenance-fence` and
`retire-maintenance-fence` apply the fence to every process in the plan.

With startup ownership, the server asks `ursulactl startup-admit` for its
initial fence state before it stamps formats, starts Raft actors or opens any
listener (`crates/ursula/src/server.rs`). The helper maps the acknowledged
reservation to one of three states. An active operation gives `activating`
with the operation's fence, and the server also starts maintenance-drained. A
completed last operation gives `retired` with that fence. An idle store
without a completion gives `awaiting_reservation`. The server refuses any other
answer, a changed incarnation or a helper failure, and does not start.

## 5. Shared reservation protocol

The reservation (`crates/ursula-ctl/src/reservation.rs`) serializes every
voter disruption in a three-voter cell. It is an offline policy. It validates
supplied evidence and proposes the next state, and it performs no Kubernetes
writes, provider calls or Raft mutations.

The store is the persistent ConfigMap `<StatefulSet>-maintenance`, with no hook
annotation, owner reference or deletion timestamp. Its `data.reservation` holds
one JSON document of schema version 1, and any other version is refused. The
document binds the cell (namespace and StatefulSet UIDs, voter IDs `{1,2,3}`,
group count, core count), a global generation, at most one operation, the last
completion receipt and an optional host inventory. A missing or recreated
store never becomes a new reservation. A new cell creates it once with
`reservation-cell`, `reservation-bootstrap --confirm-new-cell` and
`kubectl create`.

Every transition is a whole-object compare-and-swap. `reservation-request`
builds a typed request, `reservation-propose` produces the full ConfigMap with
its UID and resourceVersion, the platform submits it with `kubectl replace`,
and `reservation-acknowledge` accepts the API response only for that exact
proposal, the same store UID and a different resourceVersion. A dry run, a
lost race or a recreated store is not a receipt. Conflicts stop the executor.
`reservation-read` reads one validated field without acquiring anything.

An operation pins one source (Pod UID, Node UID, provider instance, process
incarnation), a fixed three-process plan and one executor fence. Nothing
expires. Takeover keeps the operation, source, survivor pins and admitted
prefix, and changes only the executor while raising the generation.

Prefix evidence comes from `ursulactl verify-quorum` (all three processes) or
`verify-survivors` (two processes, one named exclusion). An observation must
start after acquisition and be at most 60 seconds old at the supplied clock,
and its per-group applied prefix must not regress below the admitted
boundary or the last completion. The policy validates observations and
receipts. It does not authenticate them.

Planned Pod replacement:

| Transition | Requires | Effect |
| --- | --- | --- |
| `reserve` | Idle store, source and plan matching the last completion and host inventory | Operation and fence at the next generation |
| `admit_pod_deletion` | Fence active on all three processes, fresh full prefix | Sticky deletion admission and prefix floor |
| `bind_pod_replacement` | Original UID gone, same-name Pod with a new UID and incarnation on the same physical host | Target process pin replaced. A second binding is refused |
| `complete_pod_replacement` | Fence retired on all three processes, fresh full prefix at or above the floor | Completion receipt, operation released |

Exact-host recovery, when a voter's host is lost:

| Transition | Requires | Effect |
| --- | --- | --- |
| `publish_host_inventory` | Idle store, three Ready distinct hosts, fresh full prefix | Pre-fault catalog of Pod, Node, provider instance and zone |
| `reserve_host_recovery` | Catalogued source | Operation with pinned survivors |
| `admit_host_termination` | Fresh two-survivor prefix | Intent to terminate the original instance |
| `record_host_termination` | Observation of that exact instance as `terminated` | Sticky termination receipt |
| `admit_fenced_pod_retirement` | Pod on the original Node or none observed | Pod UID tombstone, at most 32 |
| `bind_host_replacement` | Receipt and tombstone, new Pod, Node, instance and boot in the original zone, distinct from survivors | Replacement bound |
| `admit_replacement_termination` | Bound candidate, fresh two-survivor prefix | Intent to terminate the candidate. Completion is blocked |
| `record_replacement_termination` | Candidate instance observed `terminated` | Candidate receipt |
| `restage_host_replacement` | Candidate receipt | Candidate retired, at most 8, binding cleared |
| `complete_host_replacement` | Fence retired on all three, fresh full prefix after the last termination | Catalog entry updated, operation released |

A healthy restart of the original Pod UID never satisfies a binding, so a
delayed UID-bound delete cannot destroy a voter after release. Retired Pod,
Node, instance and boot identities can never be bound again. Takeover keeps
the termination receipts, tombstones and any binding. A planned rollout
refuses to take over a host recovery.

## 6. Kubernetes integration

`server.updateStrategy = OnDelete` stages a new pod template without replacing
any pod. `server.gracefulRollout.enabled` adds a post-upgrade Job that replaces
voters one at a time from the highest ordinal down. Both are off by default.

With `gracefulRollout.maintenanceReservation = false`, the Job runs
`files/graceful-rollout.sh`. For each stale voter it runs `verify-cluster`,
`drain`, records the phase in the `<StatefulSet>-rollout-state` ConfigMap,
deletes the Pod with a UID precondition, waits for the node to return as a
caught-up voter with `ursulactl wait`, then runs `undrain` and verifies the
cluster again. The entrypoint starts the replacement drained when the state
names it `restarting`. The Job refuses to run once a
`<StatefulSet>-maintenance` store exists. Its Role can delete Pods, forward
ports, read StatefulSets and write ConfigMaps.

With `maintenanceReservation = true`, the Job runs
`files/maintenance-rollout.sh`, which requires three voters, an
`expectedGroups` equal to `raft.groupCount` and a bootstrapped store. It drives
the planned Pod transitions of section 5. A failed or cancelled Job keeps the
reservation, and the next Job takes it over. Its Role is limited to get and
update on the exact store, and a ClusterRole grants read access to Nodes and
the namespace.

`server.startupOwnership.enabled` makes every server launch, including
container restarts, run `startup-admit` (section 4). It requires the
reservation rollout, three voters, OnDelete and a dedicated non-default service
account. The chart disables automatic token mounting, projects a short-lived
token and the cluster CA, and grants get and update on the store, get on the
three named Pods and the StatefulSet, and read access to Nodes and the
namespace. The only provider is `aws-nitro`. The chart mounts
`/sys/devices/virtual/dmi/id/board_asset_tag` read-only, and the helper
requires its instance ID to match the scheduled Node's `providerID` and zone
label. An idle launch must match the catalogued Pod and host. During an
operation an unbound replacement binds its fresh boot through the same CAS
before it opens any listener, and a later restart of a bound boot is refused.

An interrupted bound host replacement has no automatic recovery. When the
replacement voter dies after its group leaders started rebuilding it, they keep
it as a learner in those groups. `verify-survivors` requires no learners, so it
refuses, and the host restaging cannot proceed. No acknowledged write is at
risk. Nothing drives host recovery automatically, and `startupOwnership` stays
off by default.

## 7. Library-only components

Two components exist in the tree, and no server or CLI path calls them:

- `crates/ursula-control` is a pure control-plane state machine with node
  registration, data group placement and one operation kernel for move,
  rebuild and decommission. It has no I/O, async or clock reads.
- `crates/ursula-raft/src/meta.rs` defines a meta OpenRaft type config, state
  machine and snapshots that apply `ursula-control` commands. Its only log
  store is the test-only `log_store/meta_test_store.rs`.
