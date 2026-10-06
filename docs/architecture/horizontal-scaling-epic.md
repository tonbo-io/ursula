# Horizontal Scaling Epic

Status: active; M1 implementation in progress under a persistent goal.
Last updated: 2026-10-06.

- Tracking issue: [#2: dynamic group membership](https://github.com/tonbo-io/ursula/issues/2).
- Design: [Horizontal scaling](horizontal-scaling.md).
- Existing foundation: [Dynamic group membership](dynamic-group-membership.md).
- Implementation baseline: upstream `main`,
  `c066d14823fc90dfa6c50e0f3d7d9e4af5c33466`.
- Dedicated branch: `docs/horizontal-scaling-epic`.
- Local worktree: `/Users/xing/Idea/ursula-horizontal-scaling`.

This file tracks scope, stories, milestone exit criteria, and reproduced
progress. The design file owns architecture and protocols. Updating a status
here does not establish runtime correctness; completed implementation stories
must link a commit/PR and the relevant validation evidence. Local drafts,
merged code, CI evidence, and live acceptance remain distinct.

## Outcome and accepted direction

An operator can add data nodes, rebalance group replicas and leaders, and
evacuate nodes while preserving acknowledged stream data and stable stream/group
identity. Scaling node count preserves each group's configured replication
policy. The initial supported RF values are 3 and 5, with a default of 3 and
optional per-group overrides. RF changes are explicit migrations.

The user accepted the independent node-count/replica-count direction and asked
for configurable RF on 2026-10-06. The detailed architecture is an engineering
draft; acceptance of that direction is not a claim that every protocol detail
has already been reviewed or implemented.

The first delivery targets persistent data WAL, durable meta Raft, fixed group
count and routing hash, a fixed initial core-count contract, and manual plan
submission with resumable server-side execution. Autopilot follows the same
executor. Online group splitting, single-stream parallel writes, memory-WAL
scaling, heterogeneous core-count migration, and physical machine provisioning
are follow-up work outside the initial contract.

## Milestone scoreboard

| Milestone | Outcome | Depends on | Status | Exit evidence |
| --- | --- | --- | --- | --- |
| M0 | Isolated worktree, RF-aware design, stories and progress ledger | None | Complete: commit `4fe583d` | Worktree/baseline verified; document links and diff checks |
| M1 | Durable control plane with persisted RF=3/5 policy | M0 | In progress | Durable restart/transport/bootstrap tests, RF/domain-policy tests |
| M2 | One supported, recoverable group/policy migration | M1 | Planned | Subset-layout move E2E, migration-boundary DST, routing/cleanup/fencing evidence |
| M3 | Operator scale-out/scale-in and bounded batch rebalance | M2 | Planned | RF=3: 3→6→3; RF=5: 5→10→5; mixed-RF E2E and capacity measurements |
| M4 | Autopilot submits safe plans through the supported executor | M3 | Planned | Deterministic policy tests, failure schedules, bounded churn and load response |

Statuses: `Planned`, `Ready`, `In progress`, `Blocked`, and `Complete`, with
evidence scope attached. Milestones advance only after their exit criteria.
An interrupted migration is a runtime operation status, not automatically a
blocked implementation milestone.

## M1 — durable control plane and replication policy

| Story | Deliverable and exit criteria | Dependencies | Status |
| --- | --- | --- | --- |
| HS-101 | Persist and restore meta vote, log, committed/applied state and snapshots; recover correctly after compaction and crash/restart without resurrecting discarded intent | M0 | Complete: commit `b7352ea` |
| HS-102 | Implement default/per-group RF=3/5 and one-domain-loss policy; infer/validate existing placements on adoption; reject invalid policies and bootstrap drift | M0 | Complete: commit `5c7e413` (deterministic policy/adoption layer) |
| HS-103 | Concrete multi-node meta transport, independent meta voters, one-time bootstrap, cluster identity and trusted client/cluster/admin node directory | HS-101, HS-102 | In progress |
| HS-104 | Ordered placement/policy projections with full-snapshot resync; data/node startup restores assignments without reinitializing from stale TOML | HS-103 | Planned |

M1 acceptance includes multi-node meta leader turnover and a durable full
restart after snapshot/log compaction. Test RF=3/quorum=2 and RF=5/quorum=3,
mixed policies, insufficient nodes, missing labels, and invalid AZ layouts
(RF=5 `2/2/1` accepted and `3/1/1` rejected under one-AZ-loss policy).
New RF settings must not silently alter static/single-node behavior. No
claim of an operator scaling workflow is made at M1.

Suggested implementation boundaries: keep deterministic policy/state in
`ursula-control`, bootstrap options in `ursula-config`, durable meta storage
and transport in `ursula-raft`, and server wiring in `ursula`. Reuse durable
storage mechanisms where suitable without assuming data-group file storage
already accepts the meta type config.

## M2 — one complete migration

| Story | Deliverable and exit criteria | Dependencies | Status |
| --- | --- | --- | --- |
| HS-201 | Intent-bound state transitions, operation idempotency, epoch CAS, source/target policy and membership-step evidence; no successful finish without verified placement | M1 | Planned |
| HS-202 | Durable dynamic group prepare/release on the owning core; joining replicas never initialize themselves; revoked replicas cannot be recreated by traffic | HS-104, HS-201 | Planned |
| HS-203 | Resumable learner catch-up, fixed-prefix applied verification, joint-membership reconciliation, leader handoff and final membership verification | HS-201, HS-202 | Planned |
| HS-204 | Durably allocated executor generations and process-incarnation fencing, receiving-process barriers and exclusion with maintenance; delayed old/raw requests cannot bypass authority | HS-201; required before managed mutations are exposed | Planned |
| HS-205 | Dynamic node/gateway routing, per-group readiness/quorum inventory, reference protection, actor/background-work retirement and safe per-core journal reclamation | HS-202, HS-203, HS-204 | Planned |
| HS-206 | Supported operation API and CLI submit/status/wait/resume with admin authority and error semantics; real-process E2E plus DST at every migration boundary | HS-201 through HS-205 | Planned |

Start with one global active operation. M2 must move a group to a node that
did not statically host it. Cover RF=3 and RF=5 replica replacements, and
explicit 3→5→3 policy changes. Capture final uniform membership, applied
prefixes, placement epochs and cleanup evidence. Exercise controller/data/meta
leader failures, destination restarts, lost replies, stale projections,
delayed executors, and snapshot pruning during install/retirement.

A dead CLI cannot lose an accepted migration. A controller failure between
membership commit and placement commit must roll forward from actual Raft
state. The underlying OpenRaft joint-consensus protocol remains authoritative.
RF=5 quorum rules must also hold in readiness, maintenance/recovery and snapshot
reference integration; no hidden two-of-three assumption is accepted.

## M3 — capacity operations

| Story | Deliverable and exit criteria | Dependencies | Status |
| --- | --- | --- | --- |
| HS-301 | Dry-run replica/leader planner respects resolved RF, failure domains, per-node headroom and movement cost; explain rejected plans | M2 | Planned |
| HS-302 | Durable resumable batches, one operation per group, bounded node/global snapshot and replication budgets; foreground load can pause transfers | HS-301 | Planned |
| HS-303 | Node draining/evacuation, independent meta-voter gate, and evidence-backed physical-removal eligibility; leader drain alone cannot pass | HS-301, HS-302 | Planned |
| HS-304 | Real-process RF=3 3→6→3 and RF=5 5→10→5 acceptance, mixed-RF layout and comparable throughput/P99/resource/transfer-cost measurements | HS-301 through HS-303 | Planned |

Verify acknowledged-prefix preservation, ordering, producer idempotency, cold
replay, retention/deletion state, stream snapshots and SSE reconnect offsets
through migrations. Check one-voter loss at RF=3 and two-voter loss at RF=5,
plus independent AZ-loss acceptance matching the configured domain policy.
Refuse scale-in that requires silently reducing RF or removing the last
required meta/data quorum. Show final resource reclamation and placement
distribution, separately from leader-count balance.

Performance comparisons keep RF, payload/workload, group count and P99 target
fixed within each scaling run. Compare RF=3 and RF=5 costs separately. Include
balanced multi-stream load, skewed groups and a single hot stream; report
measured limits rather than infer linear scaling from node count.

## M4 — autopilot

| Story | Deliverable and exit criteria | Dependencies | Status |
| --- | --- | --- | --- |
| HS-401 | Load-aware planning with stable tie-breaking, hysteresis, cooldowns and transfer budgets; submit the same durable operations as the manual API | M3 | Planned |
| HS-402 | Node-loss/replacement handling with certified executor and node-identity fencing; heartbeat expiry alone cannot retire an unreachable process | HS-401 | Planned |
| HS-403 | Operational controls/observability, bounded churn under fault/load schedules, and documented provisioning/evacuation integration | HS-401, HS-402 | Planned |

Meta-quorum loss freezes new control actions while established data groups
continue through their own quorum/read barriers. Automatic scheduling must
not override RF/domain policy, invent a new write path, or tear down an
unevacuated machine. Physical provisioning remains outside the Raft controller.

## Progress ledger

| Date | Story/milestone | Change and evidence | Scope/status |
| --- | --- | --- | --- |
| 2026-10-06 | M0 | Read issue #2 and reviewed upstream `c066d148`; recorded current raw membership APIs, dormant dynamic hosting, meta foundation, static routing/maintenance and snapshot-reference dependencies | Static review; scaling behavior not reproduced |
| 2026-10-06 | M0 | Created `docs/horizontal-scaling-epic` worktree from the pinned upstream baseline; transferred the previous design draft from the unrelated checkout | Local branch/worktree; no runtime change |
| 2026-10-06 | M0 | Added configurable default/per-group RF=3/5, explicit RF migrations, quorum arithmetic and failure-domain constraints to the design | Local design draft; policy implementation pending |
| 2026-10-06 | M0 | Established HS-101 through HS-403, dependencies, milestone exit criteria and acceptance matrices | Local tracking baseline |
| 2026-10-06 | M0 | Checked document links, diff whitespace, scoreboard/story structure and RF/AZ examples | Document checks only; no runtime tests |
| 2026-10-06 | HS-101 | User requested a persistent goal to autonomously complete the entire epic; began durable meta storage using the existing checksummed journal and filesystem-lock/durability primitives | Implementation in progress; validation pending |
| 2026-10-06 | M0 | Committed design/tracker baseline in `4fe583d` | Local committed documents |
| 2026-10-06 | HS-101 | Commit `b7352ea` adds durable meta journal, atomic checksummed snapshots, guarded purge/compaction and recoverable constructors; 8 focused tests cover process exit before/after purge and post-snapshot intent logs, torn tail, corruption, failed snapshot publication and lock exclusivity | Local implementation and reproduced process/storage tests; multi-node transport/bootstrap remain HS-103 |
| 2026-10-06 | HS-101 | `cargo fmt --all -- --check`; workspace Clippy with `-D warnings`; workspace lib/bin tests (888 passed, 1 ignored); workspace doc tests; seven DST audits; `RUSTFLAGS='--cfg madsim'` Raft lib check and `smoke_corpus_replays` | Passed; madsim has existing warnings in runtime/data-log modules; no new meta-storage warning |
| 2026-10-06 | HS-102 | Commit `5c7e413` adds numeric RF3/RF5 types, default/per-group one-domain-loss policy, atomic validated adoption of complete settled placements, immutable bootstrap configuration and persisted resolved/source/target policies. Ordinary moves preserve RF; explicit policy intents publish policy together with matching voters. Draining nodes continue serving retained groups and cannot receive new assignments | Deterministic control layer implemented; no supported data-membership mutation API exposed yet |
| 2026-10-06 | HS-102 | 12 new policy tests cover JSON/MessagePack/TOML RF validation, mixed RF, all 27 RF3 and 243 RF5 three-domain assignments, missing labels/nodes, insufficient voters, RF drift, rejected-operation atomicity, node identity/domain immutability, legacy snapshot compatibility, and explicit 3→5→3 policy publication | Reproduced control transitions; actual data-group Raft membership verification/execution remains M2 |
| 2026-10-06 | HS-102 | Extended real meta process-exit recovery to four checkpoints (legacy/managed, snapshot/purged prefix plus later logs), preserving mixed policies and source/target RF. Workspace lib/bin tests: 900 passed, 1 ignored; workspace doc tests, format, workspace Clippy, all seven DST audits and madsim smoke corpus passed | Local checks passed. Converted the crash child to the standard Tokio test entrypoint after the tracked-source DST scan flagged manual runtime construction; final audit includes staged new files |
| 2026-10-06 | HS-103 | Commit `a5af881` adds a separate `MetaRaftInternal` service and concrete network factory for append, vote, full snapshot and leader transfer. Validate protocol version, cluster token and recipient before decoding; cap messages at 16 MiB, propagate deadlines/cancellation and rebuild failed peer channels | Transport component implemented; persisted identity/bootstrap/configuration/server routing are still pending |
| 2026-10-06 | HS-103 | Two transport tests reject wrong cluster/node/version on all four RPCs and exercise three/five meta voters with mixed RF3/RF5 control policies, explicit leader transfer, one/two stopped voters, successful majority writes, a new learner receiving the purged-prefix snapshot, and a complete shutdown/reopen without membership initialization | Reproduced real TCP/tonic traffic between durable replicas in one test process; this is not yet a multi-process Ursula CLI/bootstrap E2E |
| 2026-10-06 | HS-103 | Workspace lib/bin tests: 902 passed, 1 ignored; workspace doc tests, format, workspace Clippy, seven tracked-source DST audits, madsim Raft lib check and existing smoke corpus passed. Raft suite alone: 152 passed, 1 ignored | Fixed a test race found under workspace concurrency: leader transfer can advance applied state before snapshot build, so await a covering snapshot/compacted prefix rather than exact log-ID equality. Existing smoke corpus proves compatibility, not new meta-transport fault coverage |

## Current execution checkpoint

Current implementation story: **HS-103**, concrete multi-node meta transport
and one-time bootstrap. HS-101 is implemented and validated in `b7352ea`;
HS-102's deterministic policy/adoption layer is implemented in `5c7e413`.
The separate meta RPC service and durable three/five-node transport tests are
implemented in `a5af881`. Next persist and validate the local cluster/node
bootstrap identity before accepting RPCs; add opt-in server configuration,
independent meta voters, the trusted client/cluster/admin directory, and
bootstrap wiring that seeds policy from verified uniform data memberships.
These must be wired before managed mode is usable. HS-104 then restores data
assignments from ordered projections rather than stale TOML.

M1 remains incomplete: durable storage and deterministic policy are present,
but transport has not been connected to production bootstrap/server routing,
and projection distribution is absent. Policy tests and meta recovery do not verify an actual data-group
RF migration; intent evidence, epoch CAS and receiver-side fencing remain M2.
No CI,
deployment or scaling-performance acceptance is claimed.
There is no current external blocker recorded; pending implementation is not
a blocker.

For each implementation increment, update the relevant story and this
checkpoint with the exact commit/PR, commands or CI run, reproduced results,
remaining gaps and next work. Preserve existing ledger entries. On rebase or
new upstream changes, record the new validation baseline instead of silently
relabeling old evidence. Mark `Complete` only when the stated story exit
criteria are met; milestone completion requires all constituent stories and
its cross-cutting acceptance gates.
