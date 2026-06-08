# Dynamic Group Membership Architecture and Operations

This document describes Ursula's current manual-first design for per-group
dynamic membership. It covers both the architecture and the operator workflow
for adding a node and migrating one data Raft group at a time.

The design target is not "every node hosts every group". A cluster can have
more data-capable nodes than any single data group needs:

```text
group 1 -> [node1, node2, node3]
group 2 -> [node1, node2, node4]
group 3 -> [node2, node3, node4]
group 4 -> [node1, node3, node4]
```

The current implementation provides the control-plane state and the explicit
operator steps for this layout. It does not yet include an automatic scheduler,
balancer, background migration executor, or durable meta log store.

## Goals

- Register data-capable nodes explicitly in replicated control-plane state.
- Track one placement projection per data Raft group.
- Allow one in-flight group migration at a time.
- Let one migration change one or more nodes in the same OpenRaft membership
  operation.
- Keep the data plane on OpenRaft's native learner and membership APIs instead
  of inventing a second membership protocol.
- Make every operator step inspectable and retryable through `ursulactl` and
  the underlying HTTP admin surface.

## Non-goals

- No automatic scheduler decides which group should move next.
- No automatic migration worker advances phases in the background.
- No process supervisor starts new Ursula nodes. Operators still start daemons
  through systemd, Kubernetes, Nomad, SSH, or another deployment layer.
- No stream remapping is performed by this feature. A stream still maps to a
  `RaftGroupId` through the current shard map; this feature changes which nodes
  host that group.
- No concurrent group migrations. The meta group serializes migration intent
  with a single active migration lock.

## High-level architecture

Dynamic group membership is split into three layers:

```text
operator / ursulactl
        |
        | HTTP admin calls
        v
client-plane admin routes on an Ursula node
        |
        | meta OpenRaft writes / reads
        v
meta group: control-plane state
        |
        | operator uses state to drive explicit data-plane calls
        v
data groups: OpenRaft learners and voter membership
```

The meta group owns intent and placement metadata. Data groups still own their
actual replicated stream data and their OpenRaft membership. A migration is
complete only after both are true:

1. The data group has applied the intended OpenRaft membership change.
2. The meta group has committed the placement projection that describes that
   final membership.

## Core components

### `ursula-control`

`crates/ursula-control` is a pure state-machine crate. It has no I/O, async, or
wall-clock reads. The meta Raft state machine applies `ControlCommand` values
to a `ControlPlaneState`.

The important state is:

- `nodes`: registered data-capable nodes, including client URL, cluster URL,
  labels, node state, and timestamps.
- `placements`: one `DataGroupPlacement` per `RaftGroupId`, with `voters`,
  `learners`, `draining`, `epoch`, and `updated_at_ms`.
- `migrations`: historical and active `GroupMigration` records.
- `active_migration`: the single in-flight migration id, if any.
- `next_migration_id`: monotonically assigned migration ids.

The currently relevant commands are:

- `RegisterNode`: persist a data-capable node and its URLs.
- `SeedPlacement`: record initial data-group voters during bootstrap.
- `BeginMigration`: create a migration intent and acquire the active lock.
- `CommitPlacement`: write the final placement projection after the data-group
  membership change.
- `FinishMigration`: mark the active migration as succeeded or failed and
  release the active lock.
- `EvictLearner`: remove retained learner/draining metadata later.

`BeginMigration` validates that:

- no other migration is active;
- target voters are not empty;
- every target voter is registered and migration-eligible;
- the group already has a placement.

It records `from_voters`, `target_voters`, `added_nodes`, `removed_voters`,
`retain_removed`, and learner status for newly added nodes.

### Meta OpenRaft group

`crates/ursula-raft/src/meta.rs` defines the OpenRaft type config and handle
for the meta group. The meta group replicates `ControlCommand` entries and
applies them to `ControlPlaneState`.

The HTTP admin layer never mutates control-plane state directly. It calls
`MetaRaftHandle`, which performs OpenRaft `client_write` for writes and reads
the state machine for placement projections.

Every admin endpoint that needs meta state requires the serving `HttpState` to
have a configured `MetaRaftHandle`. If it does not, the endpoint returns:

```text
meta raft is not configured for this server
```

### Data Raft groups

Each data group is an ordinary OpenRaft group that owns Durable Streams state
for its assigned stream hash range. Dynamic membership uses OpenRaft's native
operations:

- add a prepared node as a learner;
- wait for OpenRaft to replicate/catch up as needed;
- change the full voter set with one membership operation.

Ursula intentionally does not duplicate OpenRaft's membership protocol in the
meta state. The meta group records operator intent and final placement; the
data group performs the actual log membership transition.

### Local engine preparation

A node must be running before it can host a data group. Registering the node in
the meta group only records metadata; it does not start the process, create
network listeners, or initialize group-local runtime state.

Before adding a new node as a learner for a group, the operator prepares that
node's local engine:

```text
POST /__ursula/admin/groups/{raft_group_id}/local-engine
```

This allows and warms a data-group engine on the target node so the node can
receive OpenRaft traffic for that group before it has client traffic.

## Initial cluster shape

The initial bootstrap nodes are both:

- meta-group voters, which replicate control-plane state;
- data-capable nodes, which can host data Raft groups.

Initial data nodes are explicitly registered in the meta group, and each data
group gets an initial placement record. After this, newly added nodes are
registered through the same control-plane path as any other data-capable node.

## Placement model

A placement projection describes how a single data group should be served:

```text
DataGroupPlacement {
    raft_group_id,
    voters,
    learners,
    draining,
    epoch,
    updated_at_ms,
}
```

The sets have different meanings:

- `voters`: nodes that are OpenRaft voters for the group and eligible to serve
  normal data traffic.
- `learners`: non-voting replicas retained in placement metadata.
- `draining`: nodes that should be treated as non-serving during migration or
  cleanup.

`CommitPlacement` rejects inconsistent placement, such as an unregistered
voter or a node listed as both voter and learner. If the voter set changes, the
placement epoch increments.

## Migration lifecycle

A migration record is created by `BeginMigration` and completed by
`FinishMigration`. The state model has named phases for future automation:

```text
Validating
PreparingLocalEngines
AddingLearners
ChangingVoters
VerifyingMembership
CommittingPlacement
Finalizing
Succeeded
Failed
```

In the current manual-first implementation, the operator performs the external
work between begin and finish. The meta group mainly provides:

- replicated intent;
- single-migration serialization;
- target voter validation;
- final placement validation;
- an audit trail of the migration result.

## Operator workflow

The example below adds `node4` and migrates group `2` from voters `[1,2,3]` to
`[2,3,4]`, retaining `node1` as a draining learner.

### 1. Start the new Ursula node

Start the node with the deployment system before registering it. It must have:

- a stable `node_id`;
- compatible storage and cold-store configuration;
- reachable client and cluster URLs;
- the same code version expected by the cluster.

`ursulactl` does not start this process.

### 2. Register the node in the meta group

Use an admin URL on a node that has meta Raft configured:

```bash
ursulactl node register \
  --admin-url http://node1:4437 \
  --node-id 4 \
  --client-url http://node4:4437 \
  --cluster-url http://node4:4437
```

This writes `RegisterNode` to the meta group.

### 3. Inspect current placement

```bash
ursulactl group placement get \
  --admin-url http://node1:4437 \
  --raft-group-id 2
```

The response includes voters, learners, draining nodes, epoch, and node URLs.
Use this as the source of truth before deciding target voters.

### 4. Begin migration intent

```bash
ursulactl group migration begin \
  --admin-url http://node1:4437 \
  --raft-group-id 2 \
  --target-voters 2,3,4 \
  --retain-removed
```

This acquires the single active migration lock. If another migration is active,
the command is rejected.

### 5. Prepare the target node's local engine

Run this against the target node:

```bash
ursulactl group local-engine prepare \
  --admin-url http://node4:4437 \
  --raft-group-id 2
```

This warms runtime-owned group state on `node4` before OpenRaft sends it group
traffic.

### 6. Add the new node as learner

Run this against the current data-group leader:

```bash
ursulactl group learner add \
  --leader-url http://node2:4437 \
  --raft-group-id 2 \
  --node-id 4 \
  --cluster-url http://node4:4437
```

The command calls the data group's OpenRaft learner API and returns the log
index for the learner addition.

### 7. Change the voter set

OpenRaft supports changing multiple voters in one membership operation, so the
operator commits the full target voter set:

```bash
ursulactl group membership change \
  --leader-url http://node2:4437 \
  --raft-group-id 2 \
  --voters 2,3,4
```

This is the data-plane membership transition. If leadership moved after the
previous step, use the current leader URL.

### 8. Commit final placement to the meta group

```bash
ursulactl group placement commit \
  --admin-url http://node1:4437 \
  --raft-group-id 2 \
  --voters 2,3,4 \
  --learners 1 \
  --draining 1
```

This records the final control-plane projection. `learners` and `draining` may
be empty if no removed node should be retained.

### 9. Finish the migration

```bash
ursulactl group migration finish \
  --admin-url http://node1:4437 \
  --migration-id 1 \
  --success
```

Omit `--success` to finish the migration as failed and release the active lock:

```bash
ursulactl group migration finish \
  --admin-url http://node1:4437 \
  --migration-id 1
```

After finish, inspect placement again before starting the next group.

## Underlying HTTP endpoints

`ursulactl` is a thin client over these endpoints:

| Operation | Endpoint |
|-----------|----------|
| Register node | `POST /__ursula/admin/nodes/{node_id}/register?client_url=...&cluster_url=...` |
| Read placement | `GET /__ursula/admin/groups/{raft_group_id}/placement` |
| Begin migration | `POST /__ursula/admin/groups/{raft_group_id}/migrations?target_voters=...&retain_removed=...` |
| Prepare local engine | `POST /__ursula/admin/groups/{raft_group_id}/local-engine` |
| Add learner | `POST /__ursula/raft/{raft_group_id}/learners/{node_id}?addr=...` |
| Change voters | `POST /__ursula/raft/{raft_group_id}/membership?voters=...` |
| Commit placement | `POST /__ursula/admin/groups/{raft_group_id}/placement/commit?voters=...&learners=...&draining=...` |
| Finish migration | `POST /__ursula/admin/migrations/{migration_id}/finish?success=true|false` |

## Failure handling

The workflow is intentionally explicit. If a step fails:

- Do not start another group migration until the active migration is finished.
- Re-read placement and metrics before retrying the failed step.
- If learner addition or membership change failed before the final meta commit,
  either retry the data-plane step or finish the migration without `--success`.
- If the data-plane membership changed but placement commit failed, retry
  `group placement commit` with the actual voter/learner/draining sets before
  finishing the migration.
- If the target node process is missing or unreachable, start or repair the
  process first. Registering a node never starts it.

For routine restarts, prefer `ursulactl restart`; it already drains leadership
and waits for applied-index catch-up. The migration workflow is for changing
which nodes host a group.

## Current limitations

- The meta group admin handle must be configured on the node receiving meta
  admin requests.
- The current meta log-store implementation is in-memory. Replication through
  OpenRaft and disk durability for meta state are separate concerns; a durable
  meta log store is still needed before treating the meta group as a persistent
  production control plane.
- The meta state machine has phases for automation, but no automatic executor
  advances them yet.
- Only one group migration can be active at once.
- The operator chooses group order and target voters manually.
- There is no automatic rebalancer for the four-node/four-group layout.
- There is no automatic eviction workflow for retained learners or draining
  nodes beyond the lower-level control-plane command.
- The current stream-to-group mapping remains static. This feature does not
  split hot groups or move streams between group ids.

## Implementation map

- `crates/ursula-control`: control-plane state, commands, placement views, and
  migration validation.
- `crates/ursula-raft/src/meta.rs`: meta OpenRaft type config, state machine,
  snapshots, and `MetaRaftHandle`.
- `crates/ursula/src/lib.rs`: HTTP admin endpoints for node registration,
  placement reads, migration begin/finish, placement commit, local-engine
  preparation, learner addition, and membership change.
- `crates/ursula-ctl`: `ursulactl` commands that wrap the HTTP endpoints.
- `crates/ursula-raft/src/engine/factory.rs`: dynamic group hosting and
  runtime-owned group warmup used by local-engine preparation.
