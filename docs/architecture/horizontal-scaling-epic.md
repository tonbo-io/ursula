# Horizontal Scaling Epic

Status: active; M1 consumers and M2 fencing foundations in progress under a persistent goal.
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
| M2 | One supported, recoverable group/policy migration | M1 | In progress: prerequisites for HS-104 | Subset-layout move E2E, migration-boundary DST, routing/cleanup/fencing evidence |
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
| HS-103 | Concrete multi-node meta transport, independent meta voters, one-time bootstrap, cluster identity and trusted client/cluster/admin node directory | HS-101, HS-102 | Complete: commit `73f6970` (coordinated static-to-managed adoption) |
| HS-104 | Ordered placement/policy projections with full-snapshot resync; data/node startup restores assignments without reinitializing from stale TOML | HS-103 | In progress: commits `d591c26`, `73f6970`, `a5e6740`, `064a1af` (ordered transport, durable projection/assignment recovery and hosting authority) |

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
| HS-201 | Intent-bound state transitions, operation idempotency, epoch CAS, source/target policy and membership-step evidence; no successful finish without verified placement | M1 | In progress: commit `6530772` (replicated metadata protocol; physical executor integration pending) |
| HS-202 | Durable dynamic group prepare/release on the owning core; joining replicas never initialize themselves; revoked replicas cannot be recreated by traffic | HS-104, HS-201 | In progress: commits `064a1af`, `cf05e16`, `e7f12bf`, `dc54f53` (durable hosting and retirement kernel; complete prepare/release pending) |
| HS-203 | Resumable learner catch-up, fixed-prefix applied verification, joint-membership reconciliation, leader handoff and final membership verification | HS-201, HS-202 | Planned |
| HS-204 | Durably allocated executor generations and process-incarnation fencing, receiving-process barriers and exclusion with maintenance; delayed old/raw requests cannot bypass authority | HS-201; required before managed mutations are exposed | In progress: commits `6530772`, `064a1af` (durable generations and receiver lifecycle admission; managed submission/reconciliation and full maintenance integration pending) |
| HS-205 | Dynamic node/gateway routing, per-group readiness/quorum inventory, reference protection, actor/background-work retirement and safe per-core journal reclamation | HS-202, HS-203, HS-204 | In progress: commits `cf05e16`, `e7f12bf`, `dc54f53` (WAL, actor, snapshot and detached cold-work retirement; dynamic inventory and receiver integration pending) |
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


| 2026-10-06 | HS-103 | Commit `3862daa` adds checksummed, immutable durable local identity; validates cluster token, group/core count and routing hash before all meta RPC decoding; persists a canonical trusted client/cluster/admin directory and an atomic bootstrap recipe with resolved policies. Rejects meta membership/endpoint drift, incompatible snapshot installation and changes to established identity | Identity and deterministic bootstrap component implemented; production startup remains pending |
| 2026-10-06 | HS-103 | Five control tests and five bound-storage/transport tests cover RF3/RF5 meta bootstrap with mixed data policies, invalid directory/contracts/certificates, replay without resetting drained nodes, corrupt/torn/duplicate binding frames, missing journals, adoption of only empty unbound storage, and durable three-node restart after snapshot/purge | Real TCP actors in one process; data membership certificates are synthetic in these tests. Production bootstrap must collect them through data-group quorum read barriers |
| 2026-10-06 | HS-103 | Workspace lib/bin tests: 912 passed, 1 ignored; workspace doc tests, format, workspace Clippy, all seven tracked-source DST audits, madsim Raft lib check and existing smoke corpus passed | Local checks passed; no new scaling fault schedule or CLI E2E is claimed |


| 2026-10-06 | HS-103 | Commit `d591c26` extends the existing recovery barrier with opt-in uniform-membership certificates. A fresh ReadIndex must be applied; sample committed state-machine membership and reject divergence from effective membership, joint configurations, recipient/leader drift and missing capability. Bootstrap collection validates actual voter sets and canonical Raft endpoints against the complete trusted recipe under one total deadline | Quorum evidence collection and adoption helper implemented; certificates are observations, not membership-mutation reservations |
| 2026-10-06 | HS-103 | A real TCP fixture combines three durable meta voters with respectively three/five data voters, obtains actual data certificates, publishes bootstrap through meta Raft and reads it through the projection RPC. Rejects wrong committed endpoint/voter declarations, follower/legacy responses, unapplied joint membership and quorum loss; replay succeeds through independent meta quorum without resetting policy or requiring the old data layout | Real meta/data RPCs in one process; this fixture uses memory data WAL and durable meta WAL. It is not the persistent-data Ursula CLI acceptance gate |
| 2026-10-06 | HS-104 | Commit `d591c26` adds complete control projections versioned by applied meta log ID, a bound ReadProjection RPC with fresh ReadIndex/application/deadline checks, and an atomic pure cursor that rejects conflicting versions/contracts and term regression. Complete snapshots resync across arbitrarily many missed updates; stale snapshots preserve the current view | Projection transport/ordering foundation; server consumers, local cache and assignment restoration remain pending |
| 2026-10-06 | HS-104 | RF3/RF5 bound meta RPC tests cover pre-bootstrap refusal, later applied node-state visibility, compaction, follower refusal and fresh-read failure after quorum loss. Pure cursor tests cover serialization, full resync, stale/conflicting logs, missing groups/bootstrap, policy mismatch and wrong group/routing identity. Workspace lib/bin tests: 915 passed, 1 ignored; workspace doc tests, format, workspace Clippy, seven tracked-source DST audits, madsim Raft lib check and existing smoke corpus passed | Local reproduced checks; existing smoke proves compatibility, not new control-projection fault schedules |

| 2026-10-06 | HS-103 | Commit `73f6970` adds opt-in managed configuration, private meta RPC startup, immutable bootstrap recipe/coordinator cohort checks, guarded one-time meta initialization and quorum-certified adoption of existing disk data groups. Rejects invalid RF/layout/directory/path/listener contracts and disables data initialization/raw membership/import administration | Production coordinated static-to-managed adoption implemented; directly initializing fresh managed data groups remains outside this increment |
| 2026-10-06 | HS-104 | Commit `73f6970` restores data voters/cluster origins from the complete live projection, applies ordered refreshes to public routing and node readiness, and rejects disabled/removed-node startup. A pure restoration test deliberately supplies stale TOML voters | Projection consumers partially implemented; local durable recovery and dynamic hosting/maintenance inventory remain pending |
| 2026-10-06 | M1 | Real Ursula CLI processes with disk WAL: meta3/meta5, mixed RF3/RF5, default RF3/default RF5 plus override, pre/post-adoption payloads, raw-admin rejection with valid incarnation, private 64 KiB probe, per-voter metadata snapshot/purge, full restart and exact control-state preservation, non-hosting redirect to public origin, meta leader plus permitted voter loss followed by fresh control reads and new write/read | Passed in 23.74s; coordinated adoption and fixed settled layouts only; no migration/S3/performance claim |
| 2026-10-06 | M1 | Workspace lib/bin tests: 919 passed, 1 ignored; workspace doc tests, workspace Clippy with `-D warnings`, format, seven tracked-source DST audits, madsim Raft check and existing smoke corpus passed. Existing static CLI follower-forwarding test passed (1 test, 4.01s) | Corrected a private-plane probe routing gap exposed by the turnover test and a duplicate-route merge exposed by full tests; final checks passed. Existing smoke proves compatibility, not new scaling boundary fault coverage |

| 2026-10-06 | HS-104 | Commit `a5e6740` persists complete projections in a bounded checksummed checkpoint owned by the meta journal lock and bound to full local identity. Validate and order before atomic/fsynced publication; stale/conflicting views cannot roll back the recovery file. Corrupt/torn/foreign files fail startup. Publication failure closes storage and preserves the previous checkpoint | Durable recovery implemented for established settled layouts; cached state is a recovery hint, never fresh control authority |
| 2026-10-06 | HS-104 | Three storage tests cover monotonic reopen, unchanged/stale/conflicting versions, torn/corrupt/foreign checkpoints, failed publication and storage poisoning. Extended real CLI test selects meta voters {1,4,5} in distinct zones, stops all processes and restarts only data nodes {1,2,3}: fresh meta quorum read fails, both data majorities remain ready, acknowledged pre-adoption data survives and a new write/read succeeds | Reproduced independent meta/data quorum-loss recovery; CLI fixture passed in 26.61s. RF5 layout remains 2/2/1; no migration or snapshot-retirement acceptance is claimed |
| 2026-10-06 | HS-104 | Workspace lib/bin tests: 922 passed, 1 ignored; workspace doc tests, final workspace Clippy with `-D warnings`, format, all seven tracked-source DST audits, madsim Raft check and existing smoke corpus passed | Local final checks passed. A first fixture variant was rejected by the meta failure-domain constraint; corrected zones before the passing process run |

| 2026-10-06 | HS-201 | Commit `6530772` adds immutable keyed requests, source membership/policy validation, epoch and revision CAS, replay of lost responses, ordered fixed-prefix/target-applied evidence, and intent-bound publication. Publish changes voters and policy together and increments epoch once. Finish requires removed-replica cleanup plus retirement of all participant executors. Errors after authorizing possible receiver/membership side effects retain the operation lock | Replicated deterministic protocol implemented; physical receiver and data-Raft evidence production remain HS-202/HS-203/HS-204 |
| 2026-10-06 | HS-204 | Commit `6530772` durably allocates globally monotonic executor generations with claim-key idempotency and process identity. Takeover preserves irreversible progress but invalidates receiving-process, learner, verification and cleanup authority until recertified. Old tokens fail even at a matching revision. Recovered/projected snapshots validate intent/active index/generation/publication/terminal invariants | Metadata authority foundation; this is not receiving-process fencing. Managed raw mutation routes remain closed; no supported migration API is exposed |
| 2026-10-06 | HS-201/HS-204 | Eight control tests cover RF3/RF5 replacement, explicit 3→5→3 policy publication, replay/conflicting keys, stale source/epoch/revision/process evidence, incomplete/joint/unapplied target proof, generation takeover before/after publication, cleanup gates, cancellation after lost activation replies, serialization, invalid recovery records and exhausted counters | Pure tests use synthetic certificates/receipts; they prove deterministic enforcement, not actual membership movement or physical reclamation |
| 2026-10-06 | HS-201/HS-204 | Extended three-node durable meta TCP test persists an intent and receiver-activation authorization, snapshots/purges every replica, appends a lost-activation error afterward, fully restarts and verifies exact recovered control state, same-key replay, higher replacement generation and refusal of the old token without unlocking | Real meta consensus/storage/restart; synthetic data/receiver evidence. The existing combined server process fixture remains a fixed-layout adoption/recovery regression |
| 2026-10-06 | M1/M2 | Workspace lib/bin tests: 930 passed, 1 ignored; workspace doc tests, workspace Clippy with `-D warnings`, format, seven tracked-source DST audits, madsim Raft check and existing smoke corpus passed. Meta protocol v2 rejects v1 peers before decode; after that change all five meta transport tests passed (4.56s), managed CLI regression passed (32.17s), final Clippy/audits/format and madsim Raft check passed | New managed state semantics are separated from v1 peers. Existing smoke is compatibility evidence; migration-boundary DST and actual group/RF migration E2E remain pending |
| 2026-10-06 | HS-204 / HS-202 / HS-104 | Commit `064a1af` adds bound checksummed receiver checkpoints, revision CAS, persistent generation/process fences and replica assignment/tombstone FSM. Managed HTTP activation/retirement require fresh meta quorum and current process/token, survive response cancellation, exclude unrelated admin mutation and reject pending/uncertain work. Removed receivers need their own matching retired assignment before fence retirement; same-generation reopening is rejected | Receiver lifecycle admission implemented; queue processing is not a committed-prefix certificate. Actual membership submission and pending-work reconciliation remain pending |
| 2026-10-06 | HS-202 / HS-104 | Managed startup seeds settled assignments once, installs local hosting authority before warmup, restores explicitly assigned nonvoters without membership initialization, and rejects absent/retiring/retired assignments despite stale voters or the legacy dynamic allowlist | Reproduced storage/runtime foundation; no physical prepare/release, actor teardown or WAL reclamation claim |
| 2026-10-06 | HS-204 | Store tests pass across independent OS-process exit/reopen at active/pending/retired checkpoints, missing/corrupt/foreign history, stale CAS and publication I/O failure. Actual meta3 TCP/receiver HTTP test covers cancellation, replaced HTTP process identity, old generations, cleanup/tombstone gates, retirement and quorum loss | Three store tests plus one HTTP test; child entry point is ignored in the default runner and invoked explicitly. Data/other-receiver cleanup certificates are synthetic; HTTP replacement is a new in-process state |
| 2026-10-06 | M1/M2 | Workspace lib/bin tests: 936 passed, 2 ignored; workspace doc tests, Clippy with `-D warnings`, format, all seven DST audits, madsim Raft check and existing smoke passed. Static follower forwarding passed (4.74s); managed mixed-RF/meta3/meta5 CLI adoption/restart regression passed (29.17s) | Final increment checks passed. Existing smoke/CLI prove compatibility and fixed-layout recovery; migration-boundary DST, receiver binary-restart migration E2E and scale acceptance remain pending |
| 2026-10-06 | HS-202 / HS-205 | Commit `cf05e16` adds physical per-group shared-core WAL reclamation, serialized with retained groups' writes, atomic replacement/descriptor reopening, permanent old-owner lease invalidation and current-state in-process store reopening. Failed writer I/O closes subsequent commands until recovery; empty/never-written journals preserve parent fsync requirements | Low-level storage implementation; callers must first revoke hosting and drain/stop the engine. Actor/background/snapshot retirement and the supported receiver release receipt remain pending |
| 2026-10-06 | HS-202 | Three new tests plus a subprocess child reproduce retained neighbor vote/committed/purged state and later appends, direct and queued delayed old-owner refusal after a new owner opens, single-owner enforcement, unopened recovered-group reclamation, in-process reopening, failed replacement/poisoning and process-exit recovery | Actual filesystem/process storage evidence; no actual Raft membership movement or full replica cleanup claim |
| 2026-10-06 | M1/M2 | Workspace lib/bin tests: 939 passed, 3 ignored; doc tests, workspace Clippy, format, all seven DST audits, madsim Raft check and existing smoke passed. Static follower forwarding passed (4.27s), mixed-RF/meta3/meta5 CLI adoption/restart passed (27.79s). After the final nonexistent-journal parent fsync fix, all 39 log-store tests passed (3 ignored), and Clippy/format/audits passed again | Existing CLI/DST remain fixed-layout compatibility evidence. The new ignored child is explicitly invoked by its parent storage test; migration acceptance remains open |

| 2026-10-06 | HS-202 / HS-205 | Commit `e7f12bf` integrates revoked owning-core actor shutdown, coalesced cleanup independent of caller cancellation, registry/read-barrier/cache removal, snapshot lifecycle close/drain, external reference/pin retirement, snapshot metadata deletion and shared-core WAL reclamation. Retained finished builders release permits; old snapshot objects stay sealed across a new replica lifecycle | Runtime kernel implemented; external cold-work barriers and durable receiver prepare/release integration remain pending |
| 2026-10-06 | HS-202 / HS-205 | Four new tests reproduce live pin drain and failure/retry, same-pointer prefetch owner isolation, and actual disk/OpenRaft cleanup while a queued builder delays retirement, the original caller is cancelled and a neighbor group continues. Reopening is uninitialized and old handles/builders remain fenced | The native kernel fixture uses RF1; S3 reference tests use mock stores. This is not RF3/RF5 migration or receiver binary-restart acceptance |
| 2026-10-06 | M1/M2 | Workspace lib/bin tests: 943 passed, 3 ignored; doc tests, workspace Clippy, format, seven DST audits, madsim Raft check and existing smoke passed. Static follower forwarding passed (0.13s), mixed-RF/meta3/meta5 adoption/restart CLI passed (30.86s) | Existing CLI/DST remain fixed-layout compatibility evidence; full migration and scaling exit criteria remain open |

| 2026-10-06 | HS-202 / HS-205 | Commit `dc54f53` shares the permanently closeable group lifecycle across snapshot work, cold flush/GC/compaction/repair/orphan sweep, detached read materialization and external payload/snapshot publication. Admission precedes planning; detached tasks keep guards through I/O, publication and cleanup despite caller cancellation. Successful retirement also removes cold cursors/debt; revoked work cannot refill debt | Physical cleanup kernel implemented; durable assignment transitions, supported prepare/release receipts and live inventory integration remain pending |
| 2026-10-06 | HS-202 / HS-205 | Three native disk/OpenRaft tests pause actual cold flush, GC and read materialization after planning, cancel their callers, and reproduce retirement waiting while a neighbor writes. A fourth test proves cancelled HTTP writes retain their ingress byte reservation until completion | RF1 kernel fixtures with memory cold backend, not RF3/RF5 migration or real S3 acceptance. Streaming snapshot/external-payload wiring has existing protocol regression coverage; dedicated migration-fault tests remain required |
| 2026-10-06 | M1/M2 | Final workspace lib/bin tests: 947 passed, 3 ignored; doc tests, Clippy, format and seven DST audits passed. Madsim Raft check passed; after using the simulation task runtime in ingress, existing smoke passed (0.39s). SSE metrics now check actual delivered control frames across valid scheduling orders. Static forwarding and managed adoption/restart CLI passed (0.14s / 28.71s) before the ingress runtime-alias/SSE-test correction | Regressions resolved and final checks passed. CLI fixtures remain fixed-layout compatibility evidence; migration/scale/DST/performance exit criteria remain open |

## Current execution checkpoint

Current implementation stories: **HS-202/HS-104** explicit prepare/release and
intent-aware startup, alongside **HS-204/HS-203** managed submission and actual
membership reconciliation. Commit `dc54f53` extends close/drain and cancellation
isolation to detached cold work, read materialization and external payload/
snapshot publication, and retains ingress byte credit until write completion.
Commit `e7f12bf` adds cancellation-independent,
coalesced owning-core actor retirement, registry/cache/read-barrier removal,
snapshot work/pin draining and reference/metadata cleanup. Commit `cf05e16` supplies physical shared-core WAL
reclamation and permanent old log-store owner invalidation. Commit `064a1af`
supplies durable receiving-process lifecycle admission, monotonic local
assignments and retirement tombstones;
`6530772` supplies replicated intent, epoch/revision, executor generations and
evidence ordering. The supported server migration executor does not yet produce
physical data/cleanup receipts. Receiver activation/retirement is available on
the managed admin plane; raw membership/recovery, backup-import and external
maintenance-fence mutations remain closed.

Established startup restores settled data from the bound projection checkpoint
(`a5e6740`) even without meta quorum. Local assignments now override stale cached
voters and legacy hosting before actor construction; explicitly assigned
nonvoters restore without membership initialization. Dynamic prepare/release
handlers and live inventory/maintenance/snapshot consumers remain required.
Actor, snapshot, WAL and detached cold-work cleanup are integrated in the runtime
kernel. The supported receiver still needs durable prepare/release transitions,
recoverable pending-operation descriptions and physical receipts before complete
release can be certified. Queue barriers alone cannot resolve possibly committed
membership changes. Next wire durable prepare/release to this kernel, including
process-replacement recovery and exact receipt replay. Persist every managed submission,
reconcile actual data Raft membership/applied state and lost replies, and wire
physical receipts to the resumable executor.

HS-103 remains implemented in `73f6970`: opt-in configuration, independent meta3/
meta5 voters, cohort-checked one-time bootstrap and coordinated static-to-managed
adoption using actual data quorum certificates. The real-process fixture uses
mixed RF3/RF5 disk groups, confirms meta snapshot/purge, fully restarts, survives
meta-voter/leader loss and verifies reads/new writes/public-origin redirects. It
also restarts a data quorum with only a meta minority using the local checkpoint.
A fresh managed data initializer and rolling adoption are not implemented.

M1 stays open for full intent-aware consumers; M2 stays open for actual RF3/RF5
replica moves, explicit 3→5→3 and boundary fault acceptance. M3 manual scaling/
batches and M4 autopilot remain in the active goal's scope. The new receiver HTTP
test uses actual meta consensus but synthetic data/cleanup receipts and an
in-process HTTP-state replacement; it is not a physical migration or receiver
binary-restart E2E. No actual group migration, CI, deployment or scaling-performance
acceptance is claimed. No external blocker is recorded.

For each implementation increment, update the relevant story and this
checkpoint with the exact commit/PR, commands or CI run, reproduced results,
remaining gaps and next work. Preserve existing ledger entries. On rebase or
new upstream changes, record the new validation baseline instead of silently
relabeling old evidence. Mark `Complete` only when the stated story exit
criteria are met; milestone completion requires all constituent stories and
its cross-cutting acceptance gates.
