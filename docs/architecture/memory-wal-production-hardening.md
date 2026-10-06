# Memory-WAL production hardening epic

## Scope and acceptance contract

For a three-voter group, tolerate one unrecovered voter losing its entire local
state without losing acknowledged writes. Repeated losses before repair completes
count as overlapping losses. Preserve stream contents, offsets, producer deduplication,
membership, and progress through the supported single-loss recovery path.

Preservation or reconstruction of data after a quorum of voters loses state is
out of scope. Keep majority-loss recovery fail-closed and require explicit operator
acceptance of data loss. Shared S3 initialized markers prevent silent empty
reinitialization after all voters restart; they do not back up the acknowledged tail.
Dynamic voter counts, a new WAL backend, and cross-system disaster recovery are
out of scope.

Code review baseline: `c61cb60c02ebe5f9a6692ec3ec2184c5064dced5` (core Raft code
matches 0.6.2). Cloud review baseline:
`ae745b098bb974b75b958540d7c61ab6ce6e39a8`. Repository evidence does not establish
what is currently deployed. Initial fault probes are retained in the detached
worktree `/private/tmp/ursula-mem-review-c61cb60`.

## Milestones

| Order | Work | Acceptance evidence | Status |
| --- | --- | --- | --- |
| 1 | Fix early vote eligibility after memory-WAL state loss | Regression reproducing stale AppendEntries plus one state loss; acknowledged tail survives; real gRPC coverage; explanation of the recovery barrier | Implemented; real-gRPC regression and local cluster checks passed; CI/deployment pending |
| 2 | Make snapshot reference publication failures recoverable | Temporary reference PUT failure followed by successful retry without restarting the Raft group; retain snapshot GC protection | Reproduced; fix pending |
| 3 | Raft-aware maintenance gates and automatic fenced replacement | Refuse a second planned disruption until every affected group regains healthy membership and catches up; replace dead-node voters without duplicate node identities | Recovery-proof readiness added; full membership/count/disruption gates and fenced replacement pending |
| 4 | Node memory budget and backpressure | Bounded retained/uncommitted logs, hot data, request queues and concurrent rebuilds under production limits and S3 degradation; reject load before OOM | Pending |
| 5 | Business retry and Raft observability | Invalidate dead leader routes; retry only replay-safe operations within a total budget; reviewed metrics and alerts for quorum risk, stopped groups and stalled recovery; synthetic append/read | Pending |
| 6 | Production-scale qualification | Isolated fault injection with production topology/workload; measured write-recovery and full-redundancy RTO; RPO zero inside the failure model; ongoing Turn continuity; memory/disk comparison | Dedicated test cell selected; measure baselines before proposing numeric RTO targets |
| Supporting | Accurate chart/config durability docs and bounded SIGTERM handoff | Chart rendering; config documentation matches abort behavior; real-process handoff and bounded no-quorum shutdown | Implemented; local checks passed; CI/deployment pending |

Keep implementation, CI, artifact publication, deployment, and live qualification
as separate gates. Do not label any milestone complete because code merged or
because an unrelated CI suite passed.

## Initial evidence

- Existing rejoin unit tests: 10 passed.
- Review-only real-process restart without drain/prepare: passed for six groups.
  This does not cover production data volume, ongoing payload writes or Turn continuity.
- Controlled in-process three-node probe: only voter A loses state, while C is
  behind. Replaying an older valid AppendEntries opens A's vote gate; C can become
  leader without an acknowledged entry, and the surviving B stops on a log-state
  assertion. This schedule has not yet been reproduced over real gRPC.
- Snapshot install probe through the OpenRaft registry: downloading succeeds,
  reference publication fails once, and the group remains stopped after the store
  recovers. Both runs reproduced this behavior.
- Supporting changes: 194 Ursula/config unit and binary tests passed; the full
  static-cluster CLI suite passed (8 test entries; the real-S3 integration entry
  skips without its opt-in environment). The new real-process SIGTERM test verifies
  observed leadership handoff before transport shutdown, preserved acknowledged
  payloads on both survivors, and prompt clean exit after losing quorum. Clippy for
  both changed crates/all targets, formatting, Helm strict lint, memory-WAL rendering,
  opt-in rejection, termination-budget rejection and rollout shell checks passed.

## Recovery-barrier design work

The first safety fix must prevent both voting for a lagging candidate and campaigning
from a recovered prefix that is still missing the acknowledged tail. A first inbound
AppendEntries is not fresh evidence of post-restart recovery; neither taking a maximum
commit index nor replaying historical learner/promotion records proves freshness.

The implementation obtains a post-start outbound ReadIndex proof from the current leader and requires local application through its returned index before reopening voting and campaigning. Inbound AppendEntries can establish that a group exists, but cannot establish the recovery target. Disable automatic elections from bind onward and reject a TransferLeader that targets an unrecovered replica. Combine recovery eligibility with maintenance election policy so undrain cannot bypass recovery and recovery cannot clear maintenance. Readiness refuses incomplete recovery gates, and leadership balancing and SIGTERM discovery use recovery eligibility as well as the node's shed policy. Initial bootstrap is not fully recovered redundancy until these barriers finish; inject the next single-loss fault only after this startup recovery phase. Full expected-group, running-state, membership and cluster catch-up maintenance gates remain milestone 3.

Fresh initialization explicitly opens participation only after every configured voter answers empty and the shared initialized marker permits bootstrap. Explicit operator majority-loss recovery remains a separate data-loss acceptance path. Recovery proof and eligibility are process-local and cannot survive another memory-WAL restart.

The additive RejoinBarrier RPC returns a quorum-confirmed ReadIndex and committed leader vote; it does not alter replicated commands or persisted formats. During rolling upgrade, a leader lacking this RPC can supply proof through the existing 0.6.2 linearizable HEAD followed by a low-term Vote probe. The impossible empty bucket name is rejected after ReadIndex in the pinned 0.6.2 implementation; its last-log index is a conservative catch-up bound. Reject a probe target whose reported committed vote names another leader, even if its HEAD was forwarded successfully. Remove the legacy bridge when supported upgrade sources all provide RejoinBarrier. Every voter must run the repaired gate before claiming the stronger safety contract; an old voter still has the reproduced gate defect.

The real TCP/gRPC regression delivers the captured old Append through the production handler while delaying newer replication, allowing Vote traffic, and losing only A's state. It verifies that lagging C cannot win, automatic elections and TransferLeader cannot bypass A's closed gate, absent quorum cannot produce proof, and proof alone cannot open the gate before application. It repeats A's restart after repair, changes the healthy leader, rejects proof from a forwarded legacy HEAD, and verifies the acknowledged payload on all replicas. The original unpatched reproduction remains an in-process probe; the new gRPC result is validation of the repair under the equivalent schedule, not a claim of having reproduced the old binary over gRPC.

Local validation: the workspace library/binary suites passed 816 tests with one pre-existing ignored stress test, including 131 Raft tests and the real-gRPC fault schedule. The full real-process static-cluster CLI suite passed (8 entries, with real S3 opt-in absent). Workspace/all-targets Clippy, workspace doc tests, formatting, all seven DST audits and the madsim smoke corpus passed. CI, real S3 and dedicated-cell qualification remain separate gates.

## Qualification parameters

Use a dedicated test cell reusing production configuration for fault injection
(user selected). Existing production cells are read-only sampling targets until a concrete
reviewed test procedure authorizes disruptions. Reuse the production three-voter,
256-group, 3-GiB-per-voter configuration as a starting point; confirm actual cold
storage, placement, data volume, write rate and workload before treating it as equivalent.

Record separate targets and measured distributions for:

- time until successful writes resume;
- time until every group has a caught-up full voter set;
- ongoing business-operation success and total retry time;
- survivor/replacement peak memory and queue depth during rebuild;
- behavior under unavailable or delayed snapshot storage.

Measure baseline write-recovery and full-redundancy recovery first, then propose
numeric RTO targets (user selected). The observed
small-production-roll pauses of less than eight seconds and catch-up near thirty
seconds are evidence from the reported workload, not contractual upper bounds.

The existing Cloud artifact workflow provides a disposable namespace and cleanup evidence, but its candidate fixture uses 2 GiB, inline snapshots, disabled cold storage, and no gateway or indexer. It is suitable for its upgrade regression, not the selected production-config recovery baseline. Extend reviewed test automation with a separate production-config fixture, isolated S3 prefixes, concurrent append/read measurement and full-group redundancy sampling before reporting qualification results. Do not relabel the current local six-group tests or that candidate fixture as production-scale measurements.
