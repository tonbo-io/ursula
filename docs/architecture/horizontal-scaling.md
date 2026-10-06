# Horizontal Scaling with Configurable Raft Replica Sets

Status: design draft for the active [horizontal scaling epic](horizontal-scaling-epic.md),
tracked against [issue #2](https://github.com/tonbo-io/ursula/issues/2).
Code review baseline: upstream `main` at
`c066d14823fc90dfa6c50e0f3d7d9e4af5c33466` (2026-10-06).
This document specifies follow-up work; it does not claim an implemented or
tested scaling workflow. The existing [dynamic membership foundation](dynamic-group-membership.md)
remains the starting point.

## Capacity model and first release boundary

Keep stream-to-group assignment stable and move whole Raft group replicas
between nodes. Each group keeps its configured replication factor (RF), default
three and initially supporting three or five, regardless of the number of data
nodes. Balance both replicas and
leaders: moving only leaders leaves every node storing and applying every
group's writes; adding every new node as a voter increases replication work.

For example, 256 groups with three voters have 768 replica placements. On
three balanced nodes each node hosts 256 groups. On six balanced nodes each
hosts 128, with about 42 or 43 leaders. This is an ideal distribution, not a
throughput prediction. Group skew, leader work, network, snapshots, and cold
storage can limit the gain. Moving from the former layout to the latter
requires 384 replacement-replica placements, potentially substantial transfer
work even though the final replica count is unchanged.

RF=5 has 1,280 replica placements for the same 256 groups. On five balanced
nodes each hosts 256 groups; on ten each hosts 128. The RF=3 example above is
one policy, not a hard-coded limit or the topology used for RF=5 acceptance.

For balanced logical state of size D, average replicated state per node is
approximately `D * replication_factor / node_count`. With RF=3, payload
replication still sends two copies per append outside transient migration;
scale-out does not eliminate cross-AZ transfer cost. Provision memory and disk
for the extra learner, snapshot installation, and leader-failure headroom.

The first release supports manual node registration, group moves, rebalance
plans, and node evacuation, executed by a recoverable server-side controller.
It keeps `raft.group_count`, routing hash, and the deployment's core-count
contract fixed. It does not split streams, split/merge groups, or provision
machines. One hot stream still has one ordered Raft write path. A later
persisted virtual-bucket/range map can redistribute independent streams from
a hot group; it cannot parallelize one stream's ordered appends.

## Replication policy

In managed scaling mode, use a cluster default RF with optional per-group
overrides. Initially accept RF=3 and RF=5; other values are rejected rather than
silently rounded or clamped. Existing static and single-node development modes
keep their existing explicit membership semantics.

The following is a proposed bootstrap configuration, not an available option:

```toml
[control.placement]
default_replication_factor = 3
failure_domain = "zone"
survive_failure_domains = 1

[[control.placement.group_overrides]]
raft_group_id = 42
replication_factor = 5
```

Local epic implementation checkpoint (`5c7e413`): the deterministic
`ursula-control` policy layer now accepts this placement structure, validates
RF3/RF5 and one-domain-loss constraints, and persists cluster bootstrap plus
resolved per-group policies through meta snapshots/logs. Adoption validates
all recorded uniform placements atomically and rejects RF/bootstrap drift.
Ordinary migration intents preserve RF; explicit policy intents retain both
source and target policy. The server's `[control]` configuration, actual
membership discovery, multi-node transport and supported migration executor
are still subsequent stories; editing server TOML does not yet enable scaling.

Transport checkpoint (`a5af881`): meta append/vote/snapshot/leader-transfer
RPCs now use a separate service with cluster-token, recipient and protocol
checks. Real TCP tests cover three/five durable meta voters, leader handoff,
one/two unavailable voters, snapshot installation into an empty learner, and
full replica shutdown/reopen. Persisted identity, production bootstrap and
server routing remain in HS-103; these tests do not establish a supported
data-group migration or CLI scaling workflow.

Persist the default and each group's resolved policy in meta Raft. Configuration
seeds policy only on initial bootstrap; editing TOML after bootstrap must not
silently change live memberships. API/CLI policy updates are explicit, durable
operations. During static-to-managed adoption, infer and validate the existing
voter counts; reject configurations that imply an unrequested RF change.

Use `quorum = floor(RF / 2) + 1` throughout planning, readiness, maintenance,
and tests. RF=3 needs two votes and tolerates one voter failure; RF=5 needs
three and tolerates two. These are voter-failure properties, not evidence of
durability under arbitrary correlated storage or AZ failures. RF=5 also has
more replication traffic, apply work, state, and potentially quorum latency.

Failure-domain constraints are a separate policy. To keep a majority after loss
of any one AZ, each AZ may hold at most `RF - quorum` voters: one for RF=3 and
two for RF=5. Five voters can therefore use three AZs as `2/2/1`; `3/1/1` fails
that policy despite having three AZ labels. Validate old/new configurations
through joint consensus, not only the final distribution. For now the supported
failure-domain policy is loss of one domain; stronger policies require separate
placement and acceptance criteria.

Ordinary rebalance preserves each group's resolved target RF. Learners and
joint-consensus voter unions may temporarily increase the number of replicas;
they do not change the steady-state target. Never reduce RF to make a scale-in
plan fit. Reject insufficient node count, domain diversity, or capacity.

Changing a group's RF is a separate policy-migration operation. Increasing
3 to 5 adds and catches up destinations before changing voters; decreasing
5 to 3 checks the explicitly requested weaker policy before removing voters.
Persist source/target RF alongside source/target voter sets, fence and reconcile
each transition, then publish the target policy with verified placement. A
cluster-default update must explicitly state its affected groups; no background
reinterpretation of existing policy is allowed. Stage a large replacement or
RF change into verified membership steps using the same durable operation,
with an authorized intermediate target and log identity for each step.

Meta Raft has an independently configured voter set, initially three. Selecting
RF=5 for data groups does not automatically change meta membership; operators
requiring two-failure control-plane availability must also provision and
explicitly configure five meta voters.

## What exists and what is missing

| Area | Reviewed implementation | Required follow-up |
| --- | --- | --- |
| Stream routing | `StaticShardMap`: hash modulo group count, group modulo core count | Persist routing identity; reject incompatible joins and online group-count changes |
| Static replica subsets | `raft.groups`, factory ownership checks, `GroupNotHosted` | Dynamic host assignments and recoverable prepare/release |
| Raft membership | Raw add-learner and change-voter admin routes; peer address comes from `BasicNode` | Supported migration operations, reconciliation, fencing, and verification |
| Control state | `ursula-control`: nodes, placements, phases, one global active migration | Bind transitions to intent/version/evidence; idempotency; bounded history |
| Meta Raft | Type config, state machine, in-memory log and snapshots; injectable network factory | Durable vote/log/snapshot recovery, concrete multi-node transport, bootstrap and projection distribution |
| Client routing | Static node peers and per-group voters; gateway static upstream allowlist and leader cache | Live node directory, placement projection, and cache invalidation |
| Maintenance | Node expectations derive from static topology; current quorum verifier expects every voter to host every group | Per-group membership and assigned-node inventory, including joint configurations |
| Snapshot retention | Expected voters configured statically; current-reference and transfer pins exist | Membership-aware protection and explicit retirement of old references |

`allow_dynamic_group_hosting` is currently exercised only by a test. Allowing
a group in that in-memory set alone neither creates its engine nor preserves
the assignment across a restart. Likewise, `EvictLearner` changes control
metadata; it does not remove an OpenRaft learner or stop a local replica.

## Ownership and control-plane topology

```text
stream identity -> fixed group id -> cached placement and leader hint
                                      -> node -> owning core -> data Raft

operator -> admin API -> durable meta Raft intent
                         -> migration reconciler -> local replica lifecycle
                                                 -> data Raft membership
                         -> placement projections -> nodes and gateways
```

Use one small meta Raft group, initially three explicitly configured voters,
which may share machines with data nodes. Adding data capacity does not add
meta voters. Removing a machine that is also a meta voter requires a separate
meta-membership workflow first, or the operation must be rejected.

Meta Raft is authoritative for node identity, immutable routing configuration,
placement policy, migration intent, operation identity, and published placement.
Each data group's committed OpenRaft membership is authoritative for its
actual voter/learner set. The meta projection must follow that fact; publishing
placement does not grant a node Raft authority. Leader hints are ephemeral,
not meta writes on every election. Ordinary stream requests do not read or
write meta Raft synchronously.

On meta-quorum loss, freeze new placement changes and controller side effects.
Established data groups continue serving through cached projections and their
own quorum checks. An already submitted data-membership change can still
complete; recovery must discover it. A joining node without an authenticated,
validated assignment cannot initialize data groups from defaults.

Bootstrap explicitly once: create the durable meta group, register the existing
node inventory, validate each data group's actual committed membership, then
seed placements. Reject disagreement rather than overwrite live membership
from TOML. Static config remains authoritative in static mode; in dynamic mode
it supplies identity, storage, and bootstrap/discovery endpoints. Restart
recovers persisted membership and assignments, never re-seeds old placement.
Keep dynamic mode opt-in so static deployments retain their current semantics.

For the first supported scaling workflow require persistent data WAL and
durable meta storage. Memory-WAL scaling needs separate rejoin/restart evidence
against dynamic placements; existing all-node recovery assumptions do not
establish that support. Shared S3 or inline snapshots are supported migration
sources; node-local snapshot paths need an explicit transfer mechanism.

## Control state extensions and administrative authority

Extend the current model with:

- A cluster identity and immutable routing configuration: hash version, group
  count, and initial core-count contract.
- Separate client, cluster, and admin endpoints, failure-domain labels, and
  stable node identity. Use stable per-node DNS, not a load-balanced Raft
  endpoint. A fresh data replica receives a new node id; replacing an address
  must not create two independent replicas with the same identity.
- A cluster default and resolved placement policy per group: RF (3 or 5),
  failure-domain constraints and capacity weights. Each steady-state voter set
  must match its resolved RF; data RF and meta voter count are independent.
- An operation id/idempotency key, immutable source and target sets, expected
  placement epoch, executor generation, and observed membership log identity.
  Persist source/target RF and validate policy, not merely a nonempty voter set.
- Intent-bound placement commit: migration id, expected epoch, exact target
  voters, and verified final membership. `SeedPlacement` is bootstrap-only.
  `FinishMigration(success=true)` requires completed verification and placement
  publication; an error after submitting membership must retain authority until
  its outcome is reconciled.

Keep the existing global migration lock for the first end-to-end slice.
After correctness tests pass, replace it with one active operation per group
plus bounded per-node/global transfer budgets. A rebalance plan is a durable
batch of group operations, each independently resumable; it is not an atomic
cluster-wide transaction. Bound or compact completed operation history.

Only newly allocated destinations need to be `Active`. A `Draining` node
receives no new placements but may keep serving its existing groups until they
are evacuated. Do not make it ineligible for all retained voter sets or instantly
disable its current leaders. Health/heartbeat observations belong to the
controller; explicit commands carry deterministic values into the state machine.

A meta leader election is not sufficient to fence an old executor's delayed
HTTP requests. Reuse the current process-incarnation and administrative-fence
mechanisms, with generations allocated durably by meta Raft. Admission must
serialize validation, membership submission, and completion on the receiving
process; check the immutable operation and expected membership, not just the
HTTP request's epoch. Activate replacement fences and reconcile admitted work
on every process that could submit a mutation before advancing or releasing
an operation. If that barrier cannot be certified, pause the operation. A
replacement process must reject the previous process incarnation and remain
closed to control mutations until assigned current authority. Disable bypass
through the raw membership routes in managed mode. Integration must also
prevent the existing maintenance executor and scaling executor from issuing
conflicting mutations; the current local fence is a building block, not a
distributed lock by itself.

## One group move: {A, B, C} to {A, B, D}

1. **Plan and persist intent.** Check RF, AZ/rack constraints, target capacity,
   cluster/format compatibility, actual source membership, resolved group RF,
   and absence of a
   conflicting maintenance operation. Commit the intent with an epoch CAS.
2. **Prepare D.** Persist its assignment, allow hosting, warm the engine on the
   owning core, and register its Raft handle. Start it uninitialized and
   non-serving; never call `initialize` for this joining replica. Its shared
   cold namespace and snapshot backend must be accessible. Acknowledge only
   after inbound Raft RPCs can reach the engine. A restart reconstructs the
   assignment from placement plus active intent before normal serving.
3. **Add learner and catch up.** Ask the current leader to add D. Install a
   snapshot and replay the remaining log as needed. Capture a committed prefix
   L and verify both durable replication and applied state through L, with
   healthy storage and bounded current lag. Blocking `add_learner` alone is
   not proof of applied readiness. If ingress exceeds catch-up bandwidth,
   throttle or pause migration rather than promote an unhealthy destination.
4. **Change voters through OpenRaft.** Submit `{A,B,D}` using its joint-consensus
   API. Check old and target quorum viability and configured failure-domain
   guarantees throughout the transition. Prefer one replica replacement per
   operation. Do not tear down C to force removal. If C leads, first hand off
   to a caught-up voter that remains in the target set and verify the new
   leader. Temporarily retaining C as a learner is allowed but is not complete
   capacity reclamation.
5. **Verify actual membership.** Confirm the final uniform configuration is
   committed/applied, its log identity is known, and all target replicas cover
   a post-transition committed prefix. Joint membership is not a final result.
6. **Commit and distribute placement.** CAS the matching intent to the observed
   target set and increment placement epoch. Publish an ordered projection to
   nodes and gateways, with full snapshot recovery after missed updates.
7. **Release C.** Remove any retained learner from actual OpenRaft membership
   before evicting control metadata. Revoke local hosting before stopping the
   actor, unregister the handle, release caches/watchers, and prevent requests
   from lazily recreating the old engine. Drain background flush/GC/reference
   work as well as actor commands; membership fencing alone cannot undo an S3
   side effect. Certify old executor work has drained. Retire snapshot references
   safely before reclaiming local files. The durable journal is shared per core:
   reclaim group records through journal compaction, not deletion of that core's
   journal. Local replica removal never authorizes deletion of shared stream
   cold objects.
8. **Finish.** Record verification and cleanup results, then release the lock.
   Placement completion and resource cleanup should be separately observable;
   cleanup failures are retried and prevent declaring node evacuation complete.

Transfer bandwidth, snapshot build/install concurrency, additional hot bytes,
disk space, and foreground latency have explicit budgets. Pause new transfers
when those budgets are exceeded; do not repeatedly churn membership.

## Recovery and routing during transitions

| Observed state after interruption | Reconciliation |
| --- | --- |
| Intent exists, destination absent | Prepare the same destination without initializing a new cluster |
| Destination already a learner | Verify identity, resume catch-up; do not duplicate local replicas |
| Membership is joint or submission outcome unknown | Inspect committed/effective membership and outstanding work; finish the same target transition |
| Target membership committed, placement still old | Verify target state and roll forward placement; do not restore old voters from config |
| Placement published, source still hosted | Resume eviction/reference cleanup; keep node evacuation incomplete |
| Actual voters differ from both planned sets | Pause with drift evidence; require a new reconciled plan |

Cancellation before membership submission may clean up prepared learners under
the same fencing discipline. Once membership is submitted, reconcile its outcome
first. Reverting a committed change is a new migration with a new generation.

Local hosting must include committed assignments and prepared destinations in
active intents. Serving requires the group's actual Raft role and existing
linearizable read/write checks. A prepared learner is not advertised as a
client destination. During the membership/placement publication gap, routing
must accept verified new-voter leader hints; a stale placement cannot veto a
new leader or authorize an old replica to serve independently.

Replace static `ClientWriteLeaderRouter` lookups with a cached node directory
and placement view. Use `client_url` for HTTP, `cluster_url` for Raft, and
`admin_url` for mutations. The gateway's allowed destinations come from the
trusted node directory; joining D must not require redeploying a static
upstream list. Invalidate leader affinity on placement changes and failed
destinations. Bound redirects and preserve existing retry semantics: an
ambiguous non-idempotent append cannot be blindly replayed after transport
failure. Existing SSE connections may reconnect from their durable offset;
the initial guarantee is data continuity, not uninterrupted sockets.

Readiness and maintenance verification use expected assignments from the
current projection plus active migration intent, not only observed handles or
all-group static config. Verify each group's own quorum (both constituent
quorums for joint membership). Learner readiness is separate from serving
readiness. A node with zero groups can be ready for registration without
counting as a data replica.

S3 snapshot pruning needs a membership-aware reference set covering source
replicas, new destinations, retained learners, and active transfers. Disable
pruning for migrating groups in the first slice; resume only after the new
references are published and old ownership is explicitly retired. A delayed
controller or old static voter list must not make a required snapshot collectible.

## Planning scale-out and scale-in

Scale-out registers a prepared node, computes a dry-run plan, then replaces
selected replicas and balances leadership within each final voter set. Prefer
eligible failure domains and capacity headroom; among valid plans minimize
bytes moved and predicted load imbalance. Replica and leader counts alone are
insufficient: collect per-group append bytes, apply CPU, hot-state/WAL bytes,
read/SSE work, replication lag, and snapshot/cold-store pressure. Use stable
tie-breaking, movement penalties, hysteresis, and cooldowns.

Scale-in marks a node `Draining`, excludes new allocation, and evacuates all
its groups under the same migration protocol. Refuse plans with too few
remaining nodes, inadequate failure domains, or insufficient catch-up and
steady-state capacity. Transfer any meta-voter responsibility separately.
Physical deletion is allowed only after no voter/learner memberships, active
intents, local actors, or necessary snapshot references remain. Leadership
drain alone does not satisfy this gate.

Autopilot later submits the same durable plans in response to sustained load
or node loss. Provisioning remains an external responsibility (for example,
an operator scales a StatefulSet after evacuation is certified). Placement
changes do not redefine the underlying process as a disposable stateless pod.
Node-loss handling must satisfy the executor/identity fencing rules above;
heartbeat expiry alone is not proof that an unreachable old process is retired.

## API, delivery slices, and acceptance

Proposed admin operations: register/read nodes; read versioned placements;
preview/submit rebalance or drain plans; create/read/resume group migrations.
Return an operation id from durable acceptance (`202`), use idempotency keys
and expected placement epochs, and report resumable progress/errors. Internal
prepare/release and membership actions require cluster authority and process
incarnation. `ursulactl` previews/submits/waits; killing the CLI does not cancel
accepted work. These are proposed commands, not currently available interfaces.

Deliver in this order:

1. **Durable control plane and configurable RF.** Recover meta vote/log/snapshot
   atomically, add concrete transport/bootstrap, verified initial placement,
   node directory, and persisted default/per-group RF=3 or RF=5 policy.
2. **One supported move.** Harden intent transitions and fencing; wire
   prepare/release, catch-up, membership verification, dynamic routing,
   maintenance inventory, and conservative snapshot retention. Expose API/CLI.
3. **Capacity operations.** Add durable batch plans, node draining/removal,
   load-aware leader placement and bounded migration concurrency.
4. **Autopilot.** Add measured scheduling policies and external provisioning
   integration after manual plans use the same tested executor.

A raw CLI orchestration tool can be useful diagnostically before slice 1, but
does not establish the durable, multi-operator scaling contract above.

Acceptance uses real-process E2E plus DST fault schedules:

- Move a group in a configured subset layout onto a node that did not host it.
  Then expand 3 to 6 nodes and shrink 6 to 3 with RF=3 and stable stream/group
  identities. Repeat 5 to 10 to 5 with RF=5, plus a mixed-RF layout and explicit
  3 to 5 to 3 policy migrations. Verify final replica and leader distributions
  separately. With suitable independent durable storage, prove committed
  prefixes survive one voter failure at RF=3 and two at RF=5; verify the
  configured AZ-loss policy independently.
- Interrupt every migration boundary: controller crash, meta/data leader
  turnover, delayed old requests, lost responses, partitions, and destination
  restart. Verify resumption from actual committed state and rejection of
  obsolete executors. Reboot the durable meta quorum without losing intent.
- Read a recorded acknowledged append set back exactly, including hot data,
  cold data, retention/deletion state, producer idempotency, stream snapshots,
  and SSE reconnects. Check acknowledged-prefix preservation, order, and no
  accepted writes from retired replicas. Concurrent history checks cover reads
  and writes during handoff; socket continuity is measured separately.
- Run snapshot pruning concurrently with learner install and retirement;
  prove no retained reference is collected. Refuse capacity/placement-epoch
  violations, conflicting maintenance, and unsafe meta-voter removal.
- Benchmark identical multi-stream workloads at 3 and 6 nodes with fixed RF
  and group count, before/during/after migration. Report throughput at the same
  P99 target, resource use and transfer cost. Include one-hot-stream and
  skewed-group cases; do not infer linear scaling from balanced group counts.
  Repeat for five and ten nodes at RF=5; keep scalability within one RF policy
  separate from the cost/latency comparison between RF=3 and RF=5.

Issue #2's supported manual-membership checklist can close only after slice 2
passes; horizontal scale-out/in needs slice 3, and its autopilot item needs
slice 4. Foundation tests or raw membership APIs alone do not close those gates.
