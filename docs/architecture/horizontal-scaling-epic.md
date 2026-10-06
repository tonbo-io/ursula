# Horizontal Scaling Epic

Status: active; M1 consumers and M2 automatic migration acceptance in progress under a persistent goal.
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

Local device validation uses only the host target (`aarch64-apple-darwin` on
the current Mac), as requested on 2026-10-06. Do not cross-compile on this
device. `--cfg madsim` selects the simulation implementation on that same
host; it does not select a different target. Other-platform validation requires
its own native environment and remains separate evidence.

Later on 2026-10-06 the user reported that this device is on battery and
instructed all builds/tests to run on GitHub Actions Depot runners instead.
Local build/test work is suspended until the user changes that instruction;
local work is editing, review and remote-CI orchestration only. The epic's
`horizontal-scaling.yml` workflow uses native Depot Ubuntu ARM runners without
cross-target compilation. Earlier Mac results remain historical local evidence.

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

M4 is the final milestone of this epic. Completing M4 completes the epic only
when every preceding milestone, every constituent story and the cross-cutting
acceptance gates have passed with recorded evidence, and the documented change
is ready to merge. Autopilot code alone does not satisfy that definition. The
follow-up items outside the initial contract above are not completion gates.

## M1 — durable control plane and replication policy

| Story | Deliverable and exit criteria | Dependencies | Status |
| --- | --- | --- | --- |
| HS-101 | Persist and restore meta vote, log, committed/applied state and snapshots; recover correctly after compaction and crash/restart without resurrecting discarded intent | M0 | Complete: commit `b7352ea` |
| HS-102 | Implement default/per-group RF=3/5 and one-domain-loss policy; infer/validate existing placements on adoption; reject invalid policies and bootstrap drift | M0 | Complete: commit `5c7e413` (deterministic policy/adoption layer) |
| HS-103 | Concrete multi-node meta transport, independent meta voters, one-time bootstrap, cluster identity and trusted client/cluster/admin node directory | HS-101, HS-102 | Complete: commit `73f6970` (coordinated static-to-managed adoption) |
| HS-104 | Ordered placement/policy projections with full-snapshot resync; data/node startup restores assignments without reinitializing from stale TOML | HS-103 | In progress: commits `d591c26`, `73f6970`, `a5e6740`, `064a1af`, `8b88822`, `d0d3bb9`, `82e1563`, `f5124a8`, `43f48d8`, `db45b1a`, `bb7a9cd` (ordered consumers and binary assignment recovery during S3 prefetch and committed joint; M1 completion audit pending) |

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

### M1 completion audit

The audit below maps the original M1 contract to executable checks. It does
not make M2's remaining migration fault matrix a new M1 prerequisite.

| Gate | Evidence and boundary |
| --- | --- |
| HS-101: durable consensus/state recovery, compaction and crash | `ursula-raft` meta log-store tests cover OS-process exit before/after purge, post-snapshot intent logs, torn/corrupt history and failed publication. `bound_meta_bootstrap_records_replicate_and_survive_compaction_and_full_restart` restores the bound bootstrap and active executor state over actual TCP replicas. |
| HS-102: RF3/RF5, mixed policies, invalid placements and immutable adoption | `ursula-control/src/policy_tests.rs` covers both quorum sizes, all three-domain assignments, insufficient voters/nodes, missing labels, RF drift and legacy snapshots; `ursula-config/src/control.rs` covers opt-in configuration and rejected reinitialization. Domain-policy enumeration proves the policy arithmetic; it is not the M3 physical AZ-loss experiment. |
| HS-103: independent meta3/meta5, turnover, trusted identity and one-time bootstrap | `meta_transport_three_and_five_voters_survive_failures_and_full_restart`, bound transport/bootstrap checks, and `managed_cli_adopts_mixed_disk_groups_with_three_and_five_meta_voters_and_restarts`. The latter uses independent meta voters, compacts snapshots, restarts all server processes and preserves the complete recipe/policies/placement. |
| HS-104: ordered full projections and startup from persisted assignments | `complete_projection_rpc_requires_bootstrap_and_a_live_meta_quorum` verifies full-view resync after compaction and stale-view rejection. Bound cache tests reject regression, conflicting indexes and corrupt/foreign checkpoints. The managed adoption binary test restarts data voters under meta minority from cached projections without inventing fresh control authority. The outside-bootstrap migration fixture verifies settled node-7 assignment recovery with unchanged old TOML; S3-prefetch and committed-joint binary faults additionally cover active-assignment recovery. |
| Static/dev compatibility | Managed mode is opt-in. Workspace unit/bin checks include existing static/dev configuration and routing behavior; the historical focused static follower-forwarding regression passed against the implementation before the move to Depot. M1 does not claim new static-mode scaling behavior. |

The current remote run validates the implementation SHA separately from this
documentation audit. Record its terminal results before advancing the scoreboard.

## M2 — one complete migration

| Story | Deliverable and exit criteria | Dependencies | Status |
| --- | --- | --- | --- |
| HS-201 | Intent-bound state transitions, operation idempotency, epoch CAS, source/target policy and membership-step evidence; no successful finish without verified placement | M1 | In progress: commits `6530772`, `e877d48` (replicated intent and automatic physical execution; complete fault acceptance pending) |
| HS-202 | Durable dynamic group prepare/release on the owning core; joining replicas never initialize themselves; revoked replicas cannot be recreated by traffic | HS-104, HS-201 | In progress: commits `064a1af`, `cf05e16`, `e7f12bf`, `dc54f53`, `8b88822`, `e877d48`, `d0d3bb9` (physical retirement, automatic execution and real outside-bootstrap joining/restart; install-boundary acceptance pending) |
| HS-203 | Resumable learner catch-up, fixed-prefix applied verification, joint-membership reconciliation, leader handoff and final membership verification | HS-201, HS-202 | In progress: commits `3504fb5`, `e877d48`, `db45b1a`, `bb7a9cd` (RF3/RF5 replacement, explicit 3→5→3, MinIO prefetch faults and full-process committed-joint recovery; remaining snapshot/fault acceptance pending) |
| HS-204 | Durably allocated executor generations and process-incarnation fencing, receiving-process barriers and exclusion with maintenance; delayed old/raw requests cannot bypass authority | HS-201; required before managed mutations are exposed | In progress: commits `6530772`, `064a1af`, `8b88822`, `3504fb5`, `e877d48`, `f5124a8`, `43f48d8`, `bb7a9cd` (durable generations, process/pruning barriers, joint-boundary process replacement and maintenance exclusion; supported disruption lifecycle and complete delayed-request/fault acceptance pending) |
| HS-205 | Dynamic node/gateway routing, per-group readiness/quorum inventory, reference protection, actor/background-work retirement and safe per-core journal reclamation | HS-202, HS-203, HS-204 | In progress: commits `cf05e16`, `e7f12bf`, `dc54f53`, `8b88822`, `ebe8dd7`, `d0d3bb9`, `82e1563`, `f5124a8`, `43f48d8`, `db45b1a` (retirement, routing, inventory/quorum and real S3 snapshot pins/reference cleanup; complete fault acceptance pending) |
| HS-206 | Supported operation API and CLI submit/status/wait/resume with admin authority and error semantics; real-process E2E plus DST at every migration boundary | HS-201 through HS-205 | In progress: commits `e877d48`, `ebe8dd7`, `d0d3bb9`, `43f48d8`, `db45b1a`, `be6900a`, `bb7a9cd` (operation/registration API/CLI, joining, quorum observations, MinIO prefetch/joint process faults and clean CLI JSON during outages; remaining fault phases and new DST pending) |

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
| 2026-10-06 | HS-202 / HS-104 / HS-204 / HS-205 | Commit `8b88822` adds durable typed receiver prepare/release endpoints, local assignment/hosting transitions before core actions, fresh meta plus actual target data-quorum release authority, detached cancellation-independent execution and exact physical receipt replay. Activation reconciles immutable pending actions under newer generations before certifying a replacement process; target retirement settles local epoch | Receiver physical-action integration implemented; fenced membership execution, live inventories and binary-restart migration acceptance remain pending |
| 2026-10-06 | HS-202 / HS-204 | Three deterministic ledger tests, one OS-process checkpoint parent test and four actual RF3/RF5 native receiver-router tests cover pre-core prepare recovery, no self-initialization, ACK-prefix preservation, allocated-builder drain, cancellation, neighbor read/write continuity, physical cleanup, receipt replay and generation/process takeover during release | Data/meta use actual TCP Raft, data uses disk WAL/inline snapshots. Membership is directly driven in the fixture; HTTP replacement is in-process. Child storage receipts are synthetic. No supported complete migration or S3/binary-restart acceptance claim |
| 2026-10-06 | M1/M2 | Final workspace lib/bin tests: 955 passed, 3 ignored; doc tests, workspace Clippy with `-D warnings`, format and seven DST audits passed. Madsim Raft check and existing smoke passed (0.39s). Static forwarding and mixed-RF/meta3/meta5 adoption/restart CLI regressions passed (4.44s / 27.69s) against `8b88822` | Migration-boundary DST and complete operator scaling/performance remain open |
| 2026-10-06 | HS-203 / HS-204 | Commit `3504fb5` adds durable process-fenced learner/voter/handoff steps, a separate quorum-confirmed joint configuration capability, and per-replica applied-state evidence. Activation observes and reconciles pending work before certification without resubmitting it; exact step replies remain bounded and immutable within their generation | Membership endpoint integration implemented; resumable server executor/API/CLI and full migration acceptance pending |
| 2026-10-06 | HS-203 / HS-204 | Four existing RF3/RF5 physical-move fixtures now use fenced membership endpoints and actual applied evidence. Two new RF3/RF5 cases commit a real joint entry, interrupt before the uniform submission, replace HTTP process/generation, recover joint state and roll forward. A handoff test covers RF3/RF5 actual leader quorum, nonvoter refusal and exact replay. Three configuration, two bounded-ledger and one OS-exit checkpoint parent test pass | Data/meta are actual TCP Raft with local WAL/inline snapshots; HTTP replacement is in-process, joint interruption is native injection, and store facts are synthetic. No binary-restart/S3/operator scaling acceptance claim |
| 2026-10-06 | M1/M2 | Final workspace lib/bin tests: 964 passed, 3 ignored; doc tests, workspace Clippy, format, seven DST audits, madsim Raft check and existing smoke passed (0.37s). Static follower forwarding and mixed-RF/meta3/meta5 adoption/restart CLI regressions passed (5.86s / 28.04s) | Compatibility and endpoint checks passed; explicit RF changes, migration-boundary DST and complete scale/performance gates remain open |
| 2026-10-06 | HS-201 / HS-203 / HS-204 / HS-206 | Commit `e877d48` adds a bound managed-command WriteControl RPC, fresh leader routing, source-certificate capture, keyed operation API and CLI submit/status/wait/resume. The current meta leader claims a durable executor token and automatically certifies processes, prepares replicas, captures/polls fixed prefixes, performs fenced learner/handoff/voter steps, verifies/publishes placement, releases removed replicas and retires all receiver authority | Accepted intent is server-owned; cached projections and process inventory are not authority. RPC refuses legacy/bootstrap commands and followers; HTTP conflicts are 409, unavailable fresh quorums are 503 |
| 2026-10-06 | HS-203 / HS-204 / HS-206 | Two native TCP fixtures cover RF3/RF5 automatic replacements with acknowledged payload preservation, follower API submission, stopped/restarted executor tasks, actual meta leader transfer and higher generation takeover, replay/conflicting keys, CLI-client timeout/status/wait/replay, and explicit 3→5→3 policy changes with removed-data-leader handoff and physical retirement | Real meta/data Raft, disk WAL, inline snapshots and admin listeners in one process. Task interruption and meta leadership transfer do not establish OS/binary restart or S3 migration acceptance |
| 2026-10-06 | M1/M2 | Final workspace lib/bin tests: 967 passed, 3 ignored; doc tests, workspace Clippy with `-D warnings`, format, seven tracked-source DST audits, madsim Raft check and existing smoke passed (0.40s). Static follower forwarding and mixed-RF/meta3/meta5 adoption/restart CLI regressions passed (4.30s / 25.44s) | Resolved two single-step loop lint issues, released a retained test gRPC channel before WAL reopen, and made executor fixtures follow actual meta leadership/fresh quorum instead of assuming node 1 stays leader. Native compatibility checks passed; binary CLI fixtures remain fixed-layout adoption/recovery |
| 2026-10-06 | HS-205 / HS-206 | Commit `ebe8dd7` derives local readiness from the complete managed placement projection instead of boot-time voters. Maintenance report version 2 permits explicitly unassigned nodes with no resident replicas; missing assignments, unexpected replicas, incomplete RF5 membership and unknown versions fail. Static version 1 still rejects empty inventory | Two pure tests plus binary readiness before/after moves and full restart pass. This is local serving eligibility; it does not reserve disruption permission or prove physical removal eligibility |
| 2026-10-06 | HS-203 / HS-204 / HS-206 | New `ursula-ctl/tests/managed_migration_cli.rs` runs six real Ursula processes and real ursulactl submit/status/resume commands with meta3, mixed RF3/RF5 and disk WAL. With destination 5 down, accept an RF3 move, kill the allocated controller, require higher-generation takeover, and acknowledge new writes in RF3/RF5 while both processes remain down. Restart both, finish/replay, change RF 3→5→3, read pre-fault/during-fault payloads through all front doors, fully restart and compare exact control state, then acknowledge/read new data | Passed in 26.33s. Controller/destination restart happens before receiver activation because destination discovery is blocked. Inline/local snapshots; no install/joint interruption, S3, outside-bootstrap joining, scale/performance or DST claim |
| 2026-10-06 | M1/M2 | Workspace lib/bin tests: 969 passed, 3 ignored; doc tests, final Clippy with `-D warnings`, format, seven tracked-source DST audits, madsim Raft check and existing smoke (0.39s) passed. Existing mixed-RF/meta3/meta5 adoption/restart CLI passed (28.27s). Binary migration fixture uses explicitly configured static roles during pre-adoption setup; zero-role managed nodes must pass dynamic readiness | Final checks passed against `ebe8dd7`; M1/M2 cross-cutting consumers and fault acceptance remain open |

| 2026-10-06 | HS-104 / HS-202 / HS-205 / HS-206 | Commit `d0d3bb9` adds durable node registration API/CLI and immutable local identity checks before managed data startup. A seventh binary joins outside the original six-node bootstrap directory, becomes ready without data initialization, receives RF3/RF5 migrations, exposes exact native uniform membership and serves acknowledged payloads through every front door before/after full seven-process restart | Focused E2E passed in 23.68s; registration replay canonicalizes origins and refuses changed labels; bootstrap recipe and meta3 remain unchanged. Disk WAL/inline snapshots; gateway, install/joint faults, S3, capacity and DST acceptance remain open |
| 2026-10-06 | Validation | Workspace lib/bin tests: 970 passed, 3 ignored; workspace doc tests, workspace Clippy all targets with `-D warnings`, format, seven DST audits, madsim Raft lib check and smoke corpus passed. Both migration CLI fixtures passed together in 27.71s; existing managed adoption E2E passed | Local checks only. Existing madsim warnings remain; smoke corpus establishes compatibility rather than new migration fault coverage |
| 2026-10-06 | HS-104 / HS-205 / HS-206 | Commit `82e1563` adds opt-in gateway discovery from the bound meta quorum and immutable original recipe, ordered full-directory installation, resolved-group upstream selection and exact origin/node-ID redirect validation. Coalesced refresh discovers new leaders; failed refresh retains routing hints, unknown managed redirects return retryable 503, and transport failures evict affinity without replaying ambiguous writes | Real binary gateway starts before node 7 registration. A supported move to `{4,5,7}` hands leadership to retained voter 7; gateway discovers it without a periodic refresh. A second gateway observes failed refresh under meta minority while RF3/RF5 data quorums remain live, reads old payloads and acknowledges a new write that survives full seven-node restart |
| 2026-10-06 | Validation | Workspace lib/bin tests: 976 passed, 3 ignored. Final gateway tests: 87 passed; workspace Clippy/doc tests, format, seven tracked-source DST audits, madsim Raft lib check and smoke corpus passed. Both binary migration fixtures passed in 24.95s; managed adoption E2E passed in 28.21s | Local evidence only; gateway startup needs a fresh complete directory. Inline snapshots, no install/joint interruption, S3, migration DST or capacity/autopilot acceptance. Linker disk exhaustion was resolved by deleting only this worktree's generated incremental cache and rebuilding with incremental compilation disabled |
| 2026-10-06 | HS-104 / HS-204 / HS-205 | Commit `f5124a8` starts managed external pruning disabled before actors, consumes only fresh ordered projections, and drains deletion leases before receiver activation and physical retirement. Pruners retain owned leases through remote I/O despite caller cancellation. Finalizing receivers resume with exact target voters only after published placement, removed-replica cleanup and durable receiver retirement; stale views cannot roll back policy | Two OpenDAL Memory kernel tests cover source-reference removal, all RF5 target references, pause/re-enable and cancelled deletion drain with an independent neighbor. One native RF3/RF5 test covers withheld certification, executor cancellation/meta-leader takeover, higher generation and final policy using a pruning probe; this is not real S3 migration |
| 2026-10-06 | Compatibility | Meta RPC and required receiver inventory protocol are now v3. V2 meta envelopes fail before decoding and old receiver inventories cannot be certified. Disk state schemas and stream format epoch 2 are unchanged | Mixed meta RPC versions are unsupported; stop control activity for the protocol upgrade. No rolling mixed-version compatibility claim |
| 2026-10-06 | Validation | Against `f5124a8`: workspace lib/bin tests 979 passed, 3 ignored; workspace doc tests, all-target Clippy with `-D warnings`, format, seven tracked-source DST audits, madsim Raft lib check and smoke corpus (1.59s) passed. Binary migration CLI fixtures passed in 26.61s; managed adoption/restart passed in 26.70s | Native `aarch64-apple-darwin` only; server verified arm64 Mach-O. An initial workspace build lost its generated `target/` directory before tests ran; a complete rebuild passed. Incremental compilation remains disabled. Existing madsim warnings remain; these CLI/DST regressions do not establish S3/install/joint migration acceptance |
| 2026-10-06 | HS-104 / HS-204 / HS-205 | Commit `43f48d8` derives maintenance inventory v3 from placement plus durable assignment roles (voter, preparing learner, learner, retiring). Includes projection index and unresolved receiver/assignment state. Migration participants remain maintenance-ineligible; separate serving/registration eligibility uses local actual Raft membership, recovery and applied state under the allowed intent | RF3/RF5 native fixtures verify pre-core preparation, recognized learners, learner exclusion from quorum counts and HTTP serving readiness during learner/joint phases. Expected inventory never comes from observed handles. Native fixture refresh installs real fresh views explicitly; it has no production refresh task |
| 2026-10-06 | HS-205 / HS-206 | Commit `43f48d8` adds `ursulactl operation verify-quorum`: discovers the complete live directory from fresh meta state, captures native uniform/joint configuration and fixed prefixes per group, pins process identities, preserves each constituent denominator, and rechecks data/meta state before reporting. Explicit exclusions do not rewrite membership. Maintenance eligibility and full redundancy are separate from quorum availability; output never grants disruption permission | Real seven-server binary fixture uses the original six-node admin-only manifest, discovers voter 7, verifies mixed RF3/RF5, stops RF5 voters 4/5, observes three survivors against the original five, rejects a two-survivor selection, then restarts both processes. This does not certify an AZ-loss schedule, evacuation or physical removal |
| 2026-10-06 | Validation | Against `43f48d8`: workspace lib/bin tests 981 passed, 3 ignored; doc tests, all-target Clippy with `-D warnings`, format, seven DST audits, madsim Raft lib check and smoke (0.48s) passed. Final binary migration CLI fixtures passed in 30.21s; managed adoption/restart passed in 26.54s. Eight focused local inventory tests and RF3/RF5 joint tests passed | Native host only; no cross-compilation. Existing madsim warnings remain. Inline/local snapshots and native joint interruption do not prove install/joint OS-process failure, S3 or new migration-boundary DST |
| 2026-10-06 | HS-104 / HS-203 / HS-205 / HS-206 | Commit `db45b1a` adds an opt-in native MinIO fixture with real seven-server binaries, data disk WAL, independent meta Raft and S3 snapshots. A destination-only HTTP proxy blocks an actual `.snap` GET after the target publishes its durable S3 pin. RF3 kills both destination and controller and observes controller takeover; RF5 kills the destination. Both restore the original local journals, claim higher executor generations, publish exact target membership/epoch and preserve writes acknowledged before and during the fault | Verify every final voter's reference points at a readable S3 object, removed replicas have no external pointer/pins, and obsolete retained-node pins converge to at most the current pointer. Repeat after full seven-node restart; original bootstrap manifest verifies both group quorums and maintenance eligibility. Fault is external prefetch before pointer application, not a kill during durable snapshot application or joint membership |
| 2026-10-06 | Validation | Against `db45b1a`: workspace lib/bin 981 passed, 3 ignored; all three binary migration CLI tests explicitly including the opt-in test passed in 74.02s. Doc tests, all-target Clippy with `-D warnings`, format, seven tracked-source DST audits, madsim Raft lib check and existing smoke (0.43s) passed | Native `aarch64-apple-darwin` only. MinIO `RELEASE.2025-10-15T17-29-55Z` (`go1.26.3 darwin/arm64`), AWS CLI `2.33.19` (`exe/arm64`), dummy credentials and an isolated local bucket. No AWS behavior, one-hour object GC cutoff/deletion, cold-manifest replay, new migration DST, CI or deployment evidence is claimed |
| 2026-10-06 | HS-104 / HS-203 / HS-204 / HS-206 | Commit `bb7a9cd` adds a native six-process RF3/RF5 joint crash fixture. A gRPC proxy forwards native data/meta RPCs and withholds only final target-uniform membership appends. Captured joint entries contain exact source/target voter sets; the actual leader reports joint committed/applied and uniform uncommitted, while placement remains epoch 0. A neighbor group acknowledges another write at that boundary. SIGKILL all six data/meta processes, reopen the same journals, and resume through the supported CLI | Both policies reach exact uniform target membership without learners, epoch 1, higher executor generation and changed process identity. Every acknowledged payload survives, including neighbor-window writes, and another settled full restart preserves complete meta state. Inline snapshots; this does not establish the joint/S3 cross-product or a new deterministic simulation schedule. The proxy forwards AppendStream items through native unary Append RPCs, preserving correlation; it never fabricates successful Raft responses |
| 2026-10-06 | HS-206 / regression fix | Native joint crash reproduction found successful `ursulactl operation resume` stdout prefixed with temporary-unavailability WARN lines, causing strict JSON parsing to fail. Commit `be6900a` explicitly sends the shared tracing formatter to stderr, matching its documented contract | The joint fixture starts the CLI while all servers remain down, requires the actual outage warning on stderr before restarting any server, then parses the successful stdout as pure JSON. Diagnostics are drained while waiting; warnings are not filtered from stdout to make the test pass |
| 2026-10-06 | Validation | Against `bb7a9cd` including `be6900a`: workspace lib/bin 981 passed, 3 ignored; all four binary migration CLI tests, explicitly including MinIO, passed in 92.74s. Managed adoption/restart with independent three/five meta voters passed in 25.82s. All-target Clippy with `-D warnings`, doc tests, format, seven tracked-source DST audits, madsim Raft lib check and existing smoke (0.40s) passed | Native Mac host only; rebuilt server verified arm64 Mach-O. The strengthened joint fixture alone passed in 17.71s before the final combined regression. Existing madsim warnings remain. No new migration DST, CI, deployment or scaling-performance acceptance is claimed |
| 2026-10-06 | Execution constraint / remote verification | User moved all build/test work to GitHub Actions Depot because the local device is on battery. Added branch-triggered `horizontal-scaling.yml`: native ARM Rust checks, real-process migration/adoption and explicit MinIO faults, plus existing DST audits/smoke, with acceptance-log artifacts | Local compute-intensive validation is stopped. Remote results must be attached by exact SHA/run URL after completion; workflow configuration is not passing CI evidence |
| 2026-10-06 | Remote execution started | Pushed dedicated branch at `700e871b6537853efbdb434b25c0062837bd183e`; [Horizontal scaling run 37533668030](https://github.com/tonbo-io/ursula/actions/runs/37533668030) started all three Depot Ubuntu ARM jobs. Toolchain installation and seven DST audits completed successfully; native compilation/checking is running | Pending remote acceptance. Track this exact run to terminal status and fix actual failures on Depot. The workflow runs no provisioning, publication or deployment step |
| 2026-10-06 | Remote result audit | Run `37533668030` at `700e871` finished with a misleading green workflow: full logs prove 981 unit/bin passes (3 ignored), doc/format/Clippy passes, seven DST audits and existing smoke passes, and all four native migration fixtures including MinIO pass in 90.55s. The managed-adoption test actually failed immediately with `Address already in use`; its piped `tee` hid Cargo's exit status | Partial passing evidence only; the run does not satisfy the complete acceptance gate. Commit `be5c5a0` explicitly selects Bash with pipefail for every run step and reserves distinct adoption listener ports below the native runners' default ephemeral range. Revalidate on Depot before advancing M1 |
| 2026-10-06 | HS-206 simulation prerequisite | Commit `853155d` introduces explicit harness-owned restartable receiver disks under `cfg(madsim)`, reusing production CAS/authority validation with atomic before/after-commit faults. Four storage checks cover identity/exclusivity/isolation, commit-side recovery, lost prepare-receipt replay and pending work across newer generation/process | Remote validation pending. This supplies a storage seam; it is not a complete executor migration DST schedule. Bound meta persistence/transport and fault-schedule integration remain pending |
| 2026-10-06 | Remote follow-up | [Depot run 37534513860](https://github.com/tonbo-io/ursula/actions/runs/37534513860) at `e622256bd68f626726b1b4113d473dc8ddb5a0aa` passed format/Clippy, seven DST audits, all four new simulated-receiver checks and the existing smoke replay. Pipefail correctly reported failures: one positive receiver unit request hit transient meta-leader read unavailability; the joint binary fixture hit the same ephemeral listener race in its separate port helper. Three other migration fixtures passed, including MinIO | Full run failed; adoption step was skipped. Commit `0c5f3f4` adds bounded retries only for the positive learner request's pre-admission `Unavailable` meta read, leaving other errors and all authority assertions strict. Commit `8b27364` makes migration fixture ports unique and outside the native runners' default ephemeral range. Corrected remote validation remains required |

## Current execution checkpoint

Current implementation stories: **HS-104/HS-205/HS-206** dynamic live inventory,
routing/readiness, intent-aware maintenance and real-process migration
acceptance, alongside **HS-203/HS-204** migration-boundary fault/restart coverage.
Commit `e877d48` connects the durable intent and receiver protocols to an automatic
meta-leader-owned executor and supported operation API/CLI. RF3/RF5 replacement,
executor task interruption plus actual meta leadership takeover, and explicit
3→5→3 now pass through real admin HTTP and data/meta TCP Raft fixtures. Current
process discovery is a header/admission binding, never quorum authority. Fresh
meta tokens authorize every native receiver step; actual data configurations and
applied prefixes authorize publication and cleanup. Retry budgets retain bounded
receipts; further membership attempts claim a new generation instead of evicting
replies. Pending work on former leaders is reconciled before unrelated mutation.

Commit `8b88822` supplies durable replica prepare/release, physical receipts,
process/generation recovery and settled assignment epochs. `dc54f53`, `e7f12bf`
and `cf05e16` connect cold/background/reference, actor and shared-core WAL cleanup;
`064a1af` supplies receiving-process fences and `6530772` supplies meta intent,
generation and evidence ordering. `3504fb5` supplies fenced membership steps and
actual joint reconciliation; `e877d48` supplies their autonomous orchestration.
Raw managed membership/recovery, backup import and external maintenance-fence
APIs stay closed. CLI wait/resume observes the same durable ID; stopping it or
timing out does not cancel server execution. Distinct executor errors are
persisted while the operation remains active, with bounded retry backoff.

Commit `ebe8dd7` adds real-process CLI migration/RF/restart acceptance and dynamic
local readiness. An accepted intent survives actual controller loss and higher
generation takeover while the destination is unavailable. Both data groups
continue acknowledging writes through their remaining quorums. Actual receiving
process identity changes are recertified, all front doors read the acknowledged
prefixes after RF changes and full restart, and exact durable control state
survives that settled restart. These faults occur before receiver activation;
they do not establish interruption safety at snapshot install or joint consensus.

Commit `d0d3bb9` adds supported durable registration and actual joining beyond
the bootstrap directory. A new seventh process starts from the original recipe,
receives both RF3/RF5 replacements and survives full cluster restart without
extending old TOML files or changing meta voters. Registered identity must match
before data actor startup. Native quorum observations and all front-door payload
reads pass. This completes the new-node joining slice, not M1/M2 exit criteria.

Commit `82e1563` integrates gateway discovery with the trusted managed directory.
The original recipe binds meta endpoints and routing identity; complete views
install monotonically. New-node redirects trigger coalesced refresh, and exact
origin/node-ID matching replaces URL-prefix matching. A running gateway keeps
known hints after failed refresh and relies on data quorum authority. Unknown
managed redirects return retryable 503; transport failure evicts leader affinity
without replaying an ambiguous write. New gateway startup requires fresh meta
state. Two actual gateway processes cover discovery of node 7, RF3/RF5 reads,
failed refresh under meta minority, a new minority-time ACK and full restart.

Commit `f5124a8` adds live snapshot-pruning policy and receiver deletion barriers.
Managed startup pauses pruning before any actor can run, including recovery
from cached placement. Fresh settled meta views install resolved per-group
voters; active intent or local receiver fences pause the affected group.
Activation cannot certify the process until prior deletion finishes. Detached
pruners retain leases after caller cancellation, and physical retirement drains
them before references or actors are removed. Finalization resumes using target
voters after durable cleanup/retirement. Kernel tests use OpenDAL Memory and
synthetic references; the new native RF3/RF5 receiver test uses a pruning probe
with actual meta/data consensus. Real S3 migration remains an acceptance gate.
The control transport and mandatory receiver inventory are v3; mixed meta RPC
versions are unsupported and require a quiesced control upgrade. Stream format
epoch 2 and existing disk state schemas remain unchanged.

Commit `43f48d8` supplies intent-aware maintenance inventory and a supported
per-group observation command. Durable local roles distinguish preparing and
resident learners from voters and retiring actors. Maintenance readiness stays
closed for active participants or unresolved receiver work. Serving readiness
uses actual allowed Raft membership/recovery/application independently, so the
maintenance fence does not stop healthy data service. Learner existence is not
voter evidence; actual applied promotion must occur before it counts in a quorum.
The CLI captures every group's native configuration, including both joint sets,
and fixed prefixes through trusted cluster origins. A complete fresh directory
discovers outside-bootstrap nodes; pinned process metrics cover selected voters.
Final data/meta reads refuse drift. RF5 three-survivor availability remains
distinct from restored five-voter redundancy. This is read-only evidence,
not a disruption reservation or physical-removal gate.

Commit `db45b1a` exercises actual external S3 snapshot prefetch with native MinIO
and seven server processes. The receiver's durable assignment exists before
its blocked GET; the named object already has a target pin. Destination and
controller crashes at that boundary recover through the supported CLI and
server executor. Both RF3/RF5 final layouts preserve acknowledged data, exact
uniform membership and epoch, valid target references and obsolete-pin cleanup
through full restart. This covers a prefetch process fault, not interruption
during durable pointer application or joint membership; it does not test AWS,
cold-manifest replay or the one-hour object deletion cutoff.

Reproduce after building the native `ursula` binary:

```bash
CARGO_INCREMENTAL=0 cargo build -p ursula --bin ursula
CARGO_INCREMENTAL=0 cargo test -p ursula-ctl --test managed_migration_cli \
  snapshot_fault -- --ignored --nocapture
```

The fixture starts and stops its own local MinIO and creates a temporary bucket
using dummy credentials. `URSULA_MINIO_BINARY` and `URSULA_AWS_CLI` can select
native executables; `URSULA_BINARY` can select the native server executable.
Default test runs intentionally skip this external-tool fixture; the final
validation above explicitly included it.

Commits `be6900a` / `bb7a9cd` close the RF3/RF5 committed-joint OS-process crash
slice. The test observes actual committed/applied joint log positions while the
final uniform append cannot reach followers and placement remains unpublished.
All six data/meta processes stop without graceful completion, then recover the
same journals through a live `resume` CLI. Exact target membership, epoch,
process/generation replacement, acknowledged neighbor writes and a further full
restart pass. Outage warnings are now on stderr; strict JSON stdout remains
machine-readable even when the CLI begins before servers restart.

Reproduce this inline-snapshot native fixture with:

```bash
CARGO_INCREMENTAL=0 cargo build -p ursula --bin ursula
CARGO_INCREMENTAL=0 cargo test -p ursula-ctl --test managed_migration_cli \
  joint_fault -- --nocapture
```

The M1 completion audit above maps its exact exit criteria to existing checks;
its final scoreboard update awaits corrected remote adoption validation.
Next add migration-boundary DST,
remaining snapshot/prepare/release process faults, lost replies and delayed old
receiver requests. File-backed receiver open still rejects madsim; commit
`853155d` provides explicit simulated disks with shared production validation.
Integrate bound meta persistence/transport and the supported receiver/executor
into the harness without host files or a separate protocol model.
Established startup continues
restoring assigned data from its bound checkpoint even under meta minority.

M1 stays open pending its completion audit; ordered consumers, settled recovery,
meta-minority startup, S3 prefetch and committed-joint active-assignment restart
now have native evidence. M2 stays open for the remaining snapshot/recovery
boundaries, delayed requests and migration DST. Ten native fixtures exercise endpoint
and automatic execution with actual data/meta consensus, disk WAL and inline
snapshots. HTTP identity replacement and executor interruption are in-process;
separate subprocess tests cover receiver-ledger recovery independently. The
binary CLI evidence includes fixed-layout adoption/recovery, complete
single-group moves/RF changes with controller/destination recovery, and
outside-bootstrap joining with mixed-RF migrations, gateway discovery, meta
minority data continuity and full restart. Existing DST
smoke checks compatibility, not the new executor's fault boundaries.
M3 manual scaling/batches and M4 autopilot remain in the active goal's scope.
Partial native Depot CI evidence is recorded above. The first run's masked
adoption failure is fixed in source and awaits corrected remote validation;
deployment and scaling-performance acceptance are still open. No external
blocker is recorded.

For each implementation increment, update the relevant story and this
checkpoint with the exact commit/PR, commands or CI run, reproduced results,
remaining gaps and next work. Preserve existing ledger entries. On rebase or
new upstream changes, record the new validation baseline instead of silently
relabeling old evidence. Mark `Complete` only when the stated story exit
criteria are met; milestone completion requires all constituent stories and
its cross-cutting acceptance gates.
