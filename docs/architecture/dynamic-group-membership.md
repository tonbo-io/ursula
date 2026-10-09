# Dynamic Group Membership Design

This document describes the target architecture for per-group dynamic
membership and the foundation merged in the first phase. It is not an operator
runbook yet: the public HTTP admin API, `ursulactl` workflow, bootstrap wiring,
and durable meta log store are intentionally left for follow-up work.

The proposed post-0.7 convergence and PR boundaries are tracked in
[Recovery follow-ups and maintenance convergence](post-0.7-followups.md).
That proposal requires design review before enabling the new control plane.

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
- Node registration state, node lifecycle state, and hosting eligibility:
  only `Active` nodes receive new replicas (seeded voters, learners and
  promoted voters), while a source being drained may be in any state except
  `Removed`.
- One maintenance operation kernel for move, rebuild and decommission.
- Process and replica identity pins, explicit action outcomes, and evidence-checked placement completion.
- A meta Raft type config, state machine, and snapshot support, with an
  in-memory log store that only its tests use.
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
- `operations`: process and replica identity records, one active operation, and a monotonically assigned operation id.

All changes enter through `ControlPlaneState::apply(ControlCommand)`. Node
registration and initial placement seeding remain control commands. `Operation`
dispatches typed maintenance commands to the same replicated state. While an
operation is active, other mutations are rejected. Initial seeding is idempotent
but cannot overwrite an existing placement. Only evidence-checked operation
completion changes existing voters or marks a node removed.

`Begin` checks the exact affected inventory, registered participants and their
current process identities. A rebuild covers every source-hosted group; a move
names a nonempty subset; a decommission supplies replacements for the entire
inventory. Every node that gains a replica (a move or decommission target, or
a rebuild source) must be `Active` and already have an active registered
replica, because replica registration is refused while an operation is
active. The source being drained may be in any state except `Removed`. The operation
pins previous and desired voters. Executor takeover
advances a generation and invalidates observations without erasing pending
work or lowering the observed prefix floor.

The kernel accepts externally verified observations. It checks identity pins,
uniform membership, freshness, committed/applied prefix bounds and all-survivor
replica-fence installation. It does not authenticate an observation or make it
true: the future transport and driver must obtain evidence through production
Raft entry points. Process retirement in this model alone is not data-plane
fencing.

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

`Complete` requires the final uniform membership and applied-prefix evidence
for every affected group before atomically advancing placement epochs. It
rejects an unresolved action. There is no separate `CommitPlacement` or legacy
migration path that can bypass these checks.

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

## Operation and Action Lifecycle

The operation starts `Preparing`. Dispatching the first membership transition
(`AddLearner`, `ChangeVoters` or `RetireReplica`) moves it to `Reconfiguring`,
and a rebuild or decommission moves to `Retired` when its source retires.
`Abort` discards the operation only while it is `Preparing`: any pending action
was either never dispatched or cannot change membership, so the previous
placement is still accurate. In `Reconfiguring` or `Retired`, `Abort` is
refused with `Irreversible` and recovery reconciles forward. The model has no
reverse membership transition yet.

A node that completion depends on and that claims a new process instead of
restarting with its pinned identity has presumably lost its replica. The claim
is accepted, the participant pin is kept, and the operation records
`ParticipantReplaced` for that node. A blocked operation dispatches nothing
new and cannot retire or complete, and those commands return `Blocked` with
the node and reason. Before the point of no return the operator can abort it.
After it, the operation stays blocked until a later model can rebuild the lost
participant inside the operation.

Each external action has a sequence, executor process pin and exact parameters:

```text
PrepareAction -> Prepared -> MarkActionDispatched -> OutcomeUnknown
                     |                                  |
             CancelPreparedAction             verified result / drain
                     |                                  |
                NotDispatched                 finish / reassign
```

The driver must durably mark dispatch before issuing the effect. A duplicate
mark is rejected; a lost response therefore cannot authorize another dispatch.
Only a prepared action can be canceled without reconciliation. A prepared
receipt bound to a restarted process cannot be dispatched. An unknown action
survives takeover, restart and serialization. Reassignment requires an exact
drain receipt, whose authenticity and completion the future executor must
verify. Retirement of a maintenance process is not proof that its queued data
Raft effect cannot still commit.

The old migration command/state vocabulary has been removed. Old snapshots
with those fields are rejected rather than silently interpreted as an idle
new authority. This is a pre-production format boundary, not an in-place
migration mechanism.

No public operator workflow or automatic executor is enabled by this change.

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
  operation transitions, evidence checks and property tests.
- `crates/ursula-raft/src/meta.rs`: meta OpenRaft type config, state machine,
  snapshots, and `MetaRaftHandle`.
- `crates/ursula-raft/src/log_store`: the per-core journal of the data Raft
  groups. The meta Raft has no production log store yet; its tests use a
  test-only in-memory one.
