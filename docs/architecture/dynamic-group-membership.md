# Dynamic Group Membership Design

This document describes the target architecture for per-group dynamic
membership and the foundation merged in the first phase. It is not an operator
runbook yet: the public HTTP admin API, `ursulactl` workflow, bootstrap wiring,
and durable meta log store are intentionally left for follow-up work.

The design target is not "every node hosts every group". A cluster can have
more data-capable nodes than any single data group needs:

```text
group 1 -> [node1, node2, node3]
group 2 -> [node1, node2, node4]
group 3 -> [node2, node3, node4]
group 4 -> [node1, node3, node4]
```

In this phase, Ursula gains the control-plane state model and meta Raft state
machine building blocks needed to represent that layout. It does not yet expose
a supported operator surface for changing a production cluster's membership.

## Phase 1 Scope

This phase provides:

- `ursula-control`, a pure control-plane state-machine crate.
- A placement projection model for data Raft groups.
- Node registration state, node lifecycle state, and migration eligibility
  validation.
- A single active migration intent with target-voter validation.
- Placement commit and migration finish semantics.
- A meta Raft type config, state machine, snapshot support, and in-memory log
  store used for development and tests.
- Tests for the control-plane lifecycle and the meta state-machine plumbing.

This phase deliberately does not provide:

- public HTTP admin routes for dynamic membership;
- `ursulactl` commands for dynamic membership;
- production bootstrap/configuration for a meta Raft group;
- a durable on-disk meta Raft log store;
- an automatic scheduler, balancer, or migration executor;
- stream remapping between data group ids.

## Control-Plane State

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

The relevant commands are:

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

## Placement Model

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

## Target Architecture

The intended dynamic-membership system is split into three layers:

```text
operator / automation
        |
        | supported admin API and CLI (future phase)
        v
client-plane admin routes on an Ursula node (future phase)
        |
        | meta OpenRaft writes / reads
        v
meta group: control-plane state
        |
        | migration executor / operator coordination
        v
data groups: OpenRaft learners and voter membership
```

The meta group owns intent and placement metadata. Data groups still own their
actual replicated stream data and their OpenRaft membership. A migration is
complete only after both are true:

1. The data group has applied the intended OpenRaft membership change.
2. The meta group has committed the placement projection that describes that
   final membership.

Ursula intentionally does not duplicate OpenRaft's membership protocol in the
meta state. The meta group records operator intent and final placement; the
data group performs the actual log membership transition.

## Meta Raft Foundation

`crates/ursula-raft/src/meta.rs` defines the OpenRaft type config and handle
for the meta group. The meta group replicates `ControlCommand` entries and
applies them to `ControlPlaneState`.

The first phase uses an in-memory meta log store. That is sufficient for unit
tests and follow-up integration work, but it is not a persistent production
control plane. A durable meta log store and production bootstrap/configuration
path are required before meta state can be treated as cluster-critical state.

## Migration Lifecycle

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

In the first phase, these phases are state-model vocabulary only. No automatic
executor advances them, and no public operator command drives the workflow.

## Follow-Up Work

Before dynamic membership is usable on a running cluster, later PRs need to:

- add durable meta Raft storage;
- wire meta Raft into server bootstrap and configuration;
- seed initial node and group placement state from real cluster config;
- expose a supported HTTP admin surface with authentication and error semantics;
- add `ursulactl` commands over that supported surface;
- implement or document the data-plane learner/add-voter/remove-voter workflow;
- add end-to-end tests that start real multi-node clusters and move one group;
- decide whether migration progression remains manual-first or gets a
  background executor.

## Implementation Map

- `crates/ursula-control`: control-plane state, commands, placement views, and
  migration validation.
- `crates/ursula-raft/src/meta.rs`: meta OpenRaft type config, state machine,
  snapshots, and `MetaRaftHandle`.
- `crates/ursula-raft/src/log_store`: generic in-memory log-store support used
  by both data Raft and the meta Raft test foundation.
