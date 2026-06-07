# Dynamic Group Membership Design

## Summary

Ursula should support manual-first dynamic data-group placement and Raft membership
changes while keeping stream placement stable. The initial release adds an
internal meta Raft group as the authority for cluster nodes, data-group
placement, and migration state. Operators use admin API or `ursulactl` to
register nodes and migrate one data group at a time. A migration can change one
or more nodes in that group's voter set in a single OpenRaft joint-consensus
membership change.

This design intentionally does not build autopilot scheduling in v1. It stores
enough control-plane state for an autopilot to submit the same migration plans
later.

## Goals

- Support explicit node registration for nodes that can host data groups.
- Support per-group dynamic membership for data Raft groups.
- Support migrating one data group at a time to a target voter set containing
  one or more changed nodes.
- Keep removed voters as learners by default during migration, so operators can
  roll back without rebuilding those replicas.
- Persist migration state so an executor restart can resume without guessing.
- Drive runtime ownership and HTTP routing from a dynamic placement view instead
  of static startup-only per-group voters.

## Non-Goals

- No automatic self-registration of data nodes.
- No autopilot scheduling.
- No concurrent data-group migrations.
- No automatic meta-group membership expansion when new data nodes are added.
- No automatic learner eviction after a migration succeeds.
- No stream-to-group remapping. `StaticShardMap` continues to decide
  `BucketStreamId -> RaftGroupId`.

## Existing Context

The current shard layer maps a stream to a static Raft group by hashing the
bucket/stream id and applying modulo over `raft_group_count`
(`crates/ursula-shard/src/lib.rs`). The runtime maps that group to a local core
and lazily creates the corresponding group engine.

The static gRPC Raft path already has several useful pieces:

- `StaticGrpcRaftGroupEngineFactory::hosts_group()` decides whether this node
  should instantiate a group from startup-time `per_group_voters`
  (`crates/ursula-raft/src/engine/factory.rs`).
- `CoreWorker::group()` returns `RuntimeError::GroupNotHosted` when the factory
  says the node does not host a group (`crates/ursula-runtime/src/core_worker.rs`).
- `ClientWriteLeaderRouter` can turn leader hints and `GroupNotHosted` into
  HTTP redirects (`crates/ursula/src/lib.rs`).
- `RaftGroupHandleRegistry` is the local lookup table used by the gRPC Raft
  service. If a group is missing, inbound Raft RPCs fail with "not registered".

Dynamic scheduling should upgrade these pieces rather than bypassing them.

OpenRaft remains the authority for each data group's consensus membership. The
control plane only decides the intended placement and records migration progress.
OpenRaft's `add_learner()` adds a new replica before promotion, and
`change_membership(target_voters, retain)` performs joint consensus. Ursula uses
`retain=true` by default so removed voters become learners rather than being
evicted immediately.

## Architecture

### Meta Raft Group

Add one internal control Raft group, called the meta group, with a state machine
that stores control-plane data only. It does not store stream records or cold
data metadata.

The initial three bootstrap nodes are both meta voters and data-capable nodes.
New nodes are registered as data-capable nodes by writing to the meta group.
They do not automatically become meta voters.

The meta group state contains:

- `ClusterNode`
  - `node_id`
  - `client_url`
  - `cluster_url`
  - `state`: `Active`, `Draining`, `Disabled`, or `Removed`
  - registration/update timestamps
  - optional labels and capacity hints for future autopilot use
- `DataGroupPlacement`
  - `raft_group_id`
  - `voters`
  - `learners`
  - `draining`
  - `epoch`
  - `updated_at`
- `GroupMigration`
  - `migration_id`
  - `raft_group_id`
  - `from_voters`
  - `target_voters`
  - `added_nodes`
  - `removed_voters`
  - `retain_removed`: always `true` in v1
  - current phase
  - per-node learner status
  - last error and retry count
- `MetaConfig`
  - initial meta voters
  - default data replication factor
  - autopilot disabled/enabled flag reserved for future releases
  - optional capacity and failure-domain policy fields reserved for future releases

The meta group exposes a read projection used by runtime factories and HTTP
routing:

```text
GroupPlacementView {
  group_id,
  voters,
  learners,
  draining,
  epoch,
  nodes: node_id -> { client_url, cluster_url, state }
}
```

### Startup and Node Registration

Each node still starts with its own `--raft-node-id` and listen addresses. The
first three bootstrap nodes also start the meta group using static bootstrap
membership.

New nodes must be explicitly registered:

```text
ursulactl node register \
  --node-id 5 \
  --client-url http://node5:4491 \
  --cluster-url http://node5:4492
```

Registration writes the node record into the meta group. A registered node is
eligible for data group migration only when its state is `Active`.

This explicit registration is required because OpenRaft's data-group
`add_learner()` needs a `node_id -> cluster_url` mapping, and Ursula's HTTP
redirect path needs a `node_id -> client_url` mapping.

## Migration State Machine

Only one data group migration may be `Running` at a time. The migration can
change multiple voters in that one group by replacing the group voter set with
`target_voters`.

### Request

```text
raft_group_id = 1
target_voters = [2, 4, 5]
retain_removed = true
```

### Phases

1. `Validating`
   - Check `target_voters` is non-empty and contains no duplicates.
   - Check every target node exists in the meta node table.
   - Check every target node is `Active` and data-capable.
   - Check no other data group migration is running.
   - Compute `added_nodes = target_voters - current_voters`.
   - Compute `removed_voters = current_voters - target_voters`.

2. `PreparingLocalEngines`
   - For each `added_node`, call an internal admin RPC on that node to warm the
     local group engine.
   - Warming creates the local data-group Raft handle and registers it in
     `RaftGroupHandleRegistry`.
   - This phase is idempotent.

3. `AddingLearners`
   - Run on the current data-group leader.
   - For each `added_node`, call
     `raft.add_learner(node_id, BasicNode::new(cluster_url), blocking=true)`.
   - Execute learner additions sequentially in v1 to avoid several large
     snapshot catch-ups competing for S3 and network bandwidth.
   - Record per-node learner progress in the migration record.

4. `ChangingVoters`
   - Run on the current data-group leader.
   - Call `raft.change_membership(target_voters, retain=true)`.
   - OpenRaft performs joint consensus and can change multiple nodes in one
     membership operation.

5. `VerifyingMembership`
   - Read OpenRaft membership metrics from the data group.
   - Confirm the final voter set equals `target_voters`.
   - Confirm removed voters are no longer voters.
   - Confirm added voters have applied through the committed membership entry.

6. `CommittingPlacement`
   - Write `DataGroupPlacement` to the meta group:
     - `voters = target_voters`
     - `learners` includes retained removed voters and any existing learners
     - `draining` includes removed voters
     - `epoch += 1`
   - From this point, runtime ownership and HTTP routing use the new placement.

7. `Finalizing`
   - Mark the migration `Succeeded`.
   - Release the global migration lock.

### Failure and Resume

Every phase transition is written to the meta group. The executor resumes from
the last durable phase after restart.

Idempotency rules:

- Warming a group on a node is safe to repeat.
- Adding a learner that already exists is treated as success, with node metadata
  refreshed if the address changed.
- If `change_membership()` was already committed, `VerifyingMembership` detects
  the committed voter set and proceeds.
- If a migration fails before placement commit, client routing continues to use
  the old placement.
- If a migration fails after placement commit, the operator starts a new
  migration to move back to the prior voter set.

Rollback is modeled as a new migration with the previous voter set as the target.
The executor does not implicitly roll back a failed migration.

## Runtime Ownership

Replace static startup-only hosting decisions with a dynamic placement view.

```text
hosts_group(group) =
  local node is in voters or learners for group

serves_client_traffic(group) =
  local node is in voters for group
  and local node is not marked draining for group
  and local node state is Active
```

Learners can receive Raft replication and snapshots but do not accept client
traffic. Removed voters retained as learners also do not accept client traffic.

The runtime needs one new production operation:

```text
unload_group(group_id)
```

It stops the group actor, shuts down the group engine, and unregisters the local
Raft handle from `RaftGroupHandleRegistry`. This is used by explicit learner
eviction and node retirement. The existing `warm_group(group_id)` remains the
prepare step for adding learners.

## HTTP and Client Routing

`ClientWriteLeaderRouter` should read from the meta placement projection instead
of static startup peers.

Routing behavior:

- If the request lands on a non-hosting node, redirect to an active voter from
  the current placement.
- If the request lands on a learner or draining node, redirect to an active voter.
- If the request lands on a follower voter and OpenRaft returns a leader hint,
  redirect to the leader's `client_url`.
- If no leader is known during election, return `503` with `Retry-After`.
- If meta placement is temporarily unavailable, keep using a bounded stale cache
  for groups already known locally. When the cache expires, return `503` with
  `Retry-After` rather than guessing.

The redirect path uses `client_url`. Raft transport uses `cluster_url`.

## Admin API and CLI

External CLI commands:

```text
ursulactl node register --node-id 5 --client-url URL --cluster-url URL
ursulactl node list

ursulactl group placement list
ursulactl group placement get 1

ursulactl group migrate 1 --voters 2,4,5
ursulactl group migration status
ursulactl group migration resume MIGRATION_ID

ursulactl group learner evict 1 --node-id 1
```

Internal admin RPCs:

```text
POST /__ursula/admin/groups/{group_id}/warm
POST /__ursula/admin/groups/{group_id}/unload
GET  /__ursula/admin/groups/{group_id}/local-status
POST /__ursula/admin/raft/{group_id}/add-learner
POST /__ursula/admin/raft/{group_id}/change-voters
POST /__ursula/admin/raft/{group_id}/evict-learner
```

The public CLI and API always read and write through the meta group. They do not
directly mutate a data group without recording the intended operation.

## Learner Eviction

Migration keeps removed voters as learners. Permanent removal is an explicit
operation:

```text
ursulactl group learner evict 1 --node-id 1
```

Eviction flow:

1. Validate the node is not a voter for the group.
2. Call OpenRaft membership removal for the learner.
3. Update the meta placement learner set.
4. Call `unload_group()` on the removed node.

This keeps the data migration path reversible and separates "move traffic" from
"delete replica".

## Testing Strategy

### Control State Machine

- Registering a node persists `client_url`, `cluster_url`, and state.
- Registering an existing node updates URLs without changing `node_id`.
- A migration lock prevents two running migrations.
- Migration phases resume from the last durable phase.
- Placement epoch increments only during `CommittingPlacement`.

### Raft Integration

- A warmed target node can receive data-group Raft RPCs.
- `add_learner(blocking=true)` catches up a new node.
- `change_membership(target_voters, retain=true)` changes multiple voters for a
  single group.
- Removed voters are not voters after migration and remain available as learners
  until eviction.

### Runtime

- Dynamic `hosts_group()` returns true for voters and learners.
- Learners do not serve client traffic.
- Draining retained learners do not serve client traffic.
- `unload_group()` removes the actor and unregisters the Raft handle.

### HTTP End-to-End

- Requests to a non-hosting node redirect to a current active voter.
- Requests to a learner redirect to a current active voter.
- Requests to a follower voter redirect to the leader when a leader hint exists.
- During election, leader-unknown errors return `503 Retry-After`.
- After placement commit, client traffic routes only to the new voters.

## Implementation Shape

The implementation should be split into bounded modules:

- `crates/ursula-control`: meta group command types, state machine, placement
  view, and migration records.
- `crates/ursula-raft`: wrappers around OpenRaft learner, voter change, learner
  eviction, and membership inspection for data groups.
- `crates/ursula-runtime`: dynamic placement-aware hosting decisions and
  production `unload_group()`.
- `crates/ursula`: admin routes, HTTP router integration, migration executor,
  and meta placement cache.
- `crates/ursula-ctl`: CLI commands for node registration, placement inspection,
  migration submission/resume, and learner eviction.

The exact crate split can be adjusted during planning, but the control state
machine, runtime ownership, and HTTP routing responsibilities should remain
separate.

## Operational Notes

- Production dynamic membership should use durable Raft log storage, not
  in-memory Raft.
- Meta group unavailability should not cause speculative data placement changes.
- Operators should run migrations during periods where old and new voters have
  enough healthy overlap for joint consensus.
- Large learner catch-up may install snapshots and should be observable through
  migration status and existing Raft metrics.

## Future Work

- Autopilot scheduler that generates the same migration requests based on load,
  capacity, and failure domains.
- Dynamic meta-group membership operations.
- Concurrency for independent group migrations once resource admission and
  conflict checks are defined.
- Automatic learner eviction policy after a configurable retention window.
- Node heartbeats and health-driven node state transitions.
