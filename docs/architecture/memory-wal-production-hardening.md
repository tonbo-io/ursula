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
| 1 | Fix early vote eligibility after memory-WAL state loss | Regression reproducing stale AppendEntries plus one state loss; acknowledged tail survives; real gRPC coverage; explanation of the recovery barrier | Implemented; local and remote CI passed at 874c3a4; EKS qualification/deployment pending |
| 2 | Make snapshot reference publication failures recoverable | Temporary reference PUT failure followed by successful retry without restarting the Raft group; retain snapshot GC protection | Implemented; local and remote CI passed at 874c3a4; EKS qualification/deployment pending |
| 3 | Raft-aware maintenance gates and automatic fenced replacement | Refuse a second planned disruption until every affected group regains healthy membership and catches up; replace dead-node voters without duplicate node identities | Configuration-backed local eligibility and CLI gates implemented; serialized disruption, fresh cluster proof and fenced replacement pending |
| 4 | Node memory budget and backpressure | Bounded retained/uncommitted logs, hot data, request queues and concurrent rebuilds under production limits and S3 degradation; reject load before OOM | Pending |
| 5 | Business retry and Raft observability | Invalidate dead leader routes; retry only replay-safe operations within a total budget; reviewed metrics and alerts for quorum risk, stopped groups and stalled recovery; synthetic append/read | Pending |
| 6 | Production-scale qualification | Isolated fault injection with production topology/workload; measured write-recovery and full-redundancy RTO; RPO zero inside the failure model; ongoing Turn continuity; memory/disk comparison | Dedicated test cell selected; measure baselines before proposing numeric RTO targets |
| Supporting | Accurate chart/config durability docs and bounded SIGTERM handoff | Chart rendering; config documentation matches abort behavior; real-process handoff and bounded no-quorum shutdown | Implemented; local and remote CI passed at 874c3a4; EKS qualification/deployment pending |

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

The implementation obtains a post-start outbound ReadIndex proof from the current leader and requires local application through its returned index before reopening voting and campaigning. Inbound AppendEntries can establish that a group exists, but cannot establish the recovery target. Disable automatic elections from bind onward and reject a TransferLeader that targets an unrecovered replica. Combine recovery eligibility with maintenance election policy so undrain cannot bypass recovery and recovery cannot clear maintenance. Readiness refuses incomplete recovery gates, and leadership balancing and SIGTERM discovery use recovery eligibility as well as the node's shed policy. Initial bootstrap is not fully recovered redundancy until these barriers finish; inject the next single-loss fault only after this startup recovery phase. Further maintenance gates are described below.

Fresh initialization explicitly opens participation only after every configured voter answers empty and the shared initialized marker permits bootstrap. Explicit operator majority-loss recovery remains a separate data-loss acceptance path. Recovery proof and eligibility are process-local and cannot survive another memory-WAL restart.

The additive RejoinBarrier RPC returns a quorum-confirmed ReadIndex and committed leader vote; it does not alter replicated commands or persisted formats. During rolling upgrade, a leader lacking this RPC can supply proof through the existing 0.6.2 linearizable HEAD followed by a low-term Vote probe. The impossible empty bucket name is rejected after ReadIndex in the pinned 0.6.2 implementation; its last-log index is a conservative catch-up bound. Reject a probe target whose reported committed vote names another leader, even if its HEAD was forwarded successfully. Remove the legacy bridge when supported upgrade sources all provide RejoinBarrier. Every voter must run the repaired gate before claiming the stronger safety contract; an old voter still has the reproduced gate defect.

The real TCP/gRPC regression delivers the captured old Append through the production handler while delaying newer replication, allowing Vote traffic, and losing only A's state. It verifies that lagging C cannot win, automatic elections and TransferLeader cannot bypass A's closed gate, absent quorum cannot produce proof, and proof alone cannot open the gate before application. It repeats A's restart after repair, changes the healthy leader, rejects proof from a forwarded legacy HEAD, and verifies the acknowledged payload on all replicas. The original unpatched reproduction remains an in-process probe; the new gRPC result is validation of the repair under the equivalent schedule, not a claim of having reproduced the old binary over gRPC.

Local validation: the workspace library/binary suites passed 816 tests with one pre-existing ignored stress test, including 131 Raft tests and the real-gRPC fault schedule. The full real-process static-cluster CLI suite passed (8 entries, with real S3 opt-in absent). Workspace/all-targets Clippy, workspace doc tests, formatting, all seven DST audits and the madsim smoke corpus passed. CI, real S3 and dedicated-cell qualification remain separate gates.

## Snapshot reference recovery

The reproduced reference PUT failure was inside OpenRaft's state-machine worker,
where an I/O error permanently stops the group. Prepare and pin an incoming S3
pointer before downloading or entering Raft installation. The state-machine
worker installs the already decoded snapshot and commits the local pointer;
publish its current reference after leaving that worker. A temporary publication
failure returns a retryable install RPC error while the accepted pointer stays
pinned and Raft remains live. Retrying on the same group publishes the reference,
including when Raft ignores an already installed snapshot.

Pins use the existing version-2 reference JSON under each group's recursive
`references/` prefix. The 0.6.2 collector already scans that prefix, so no snapshot
or reference format migration is needed. Retain the current pointer and every
prepared build/install pointer; retire only this node identity's obsolete pins.
Keep the current pin after publication because a newer pointer can become current
while an older PUT is in flight. Serialize reference I/O without holding that
lock across a Raft call. Reconcile abandoned pins on the next preparation or
publication, including fenced durable restart; no unbounded process-local history
of retired pointers is retained. This assumes one live owner of a node identity;
fencing replacement owners remains milestone 3. Snapshot object upload and pin
creation remain separate operations subject to the existing GC grace contract.

Snapshot builders and installers commit metadata and current pointers under the
same lock. A builder captured before a newer installation returns the newer
snapshot rather than overwriting its pointer or durable metadata. A pin failure
happens before installation; a builder may use its existing inline fallback.
Failure to publish an already pinned external pointer does not force a potentially
large inline copy. Restored durable pointers also keep their pin if publication
temporarily fails. Permanent pin/download or local metadata failures retain their
existing failure behavior.

Fault regressions cover pre-install pin failure, post-install current-reference
PUT failure, retry without restarting Raft, and a new committed command on the
same handle. Additional tests cover rejected installs releasing their pins,
concurrent older PUT/new current pointer transitions, an old builder racing a
new installation, and zero-grace collection with the actual S3 snapshot-store
implementation over an OpenDAL memory backend. These are not live AWS fault tests
or production-scale measurements.

## CI and simulation follow-up

Draft PR [#370](https://github.com/tonbo-io/ursula/pull/370) at
`89e7536cd00ab1df69d957c4e15aa86a11c21a58` passed the Rust, lint, Helm, documentation,
protocol, real-S3 integration, three-node SQLite VFS E2E, memory/disk soak,
state-growth and candidate-artifact checks. Its DST job failed: the rejoin fixture
had not driven the new outbound recovery proof. Simulation now shares the
production proof driver and cancels process-owned background tasks when a simulated
process dies, before starting its replacement.

That coverage also exposed a manual majority-loss recovery interaction: ReadIndex
uses AppendEntries, but its Conflict response does not rewind replication progress.
Do not consume the network's operator override on that probe. End the override
when replication metrics reset, or a successful response proves restoration of
the former prefix even if a fast rebuild occurred between metrics samples. Clear
it on a leader change. Unit tests and the majority-loss scenario cover a subsequent
single loss after operator recovery so authorization cannot carry into that loss.
This preserves the existing explicit data-loss-acceptance path; it does not add
a quorum-loss data preservation guarantee.

The updated workspace library/binary suites passed 823 tests with one pre-existing
ignored stress test. Reference/GC/race regressions, workspace doc tests and all
seven DST audits passed locally. Workspace/all-targets Clippy, formatting and all
eight static-cluster CLI entries passed (the real-S3 entry skipped without opt-in).
The unchanged CI scripts for smoke corpus/PR seeds, bounded-state seed families,
and memory-WAL scenarios passed locally. All three memory-WAL families also passed
seeds 1–32, including the subsequent single loss. Remote checks must run again on
the updated head before their results apply to the snapshot-reference changes.

At `ad40f9d1ad30be0b6d698fc2fefa2edf38397078`, remote DST, amd64 Rust,
lint, Helm, documentation, protocol, real-S3 integration, SQLite VFS E2E,
memory/disk soak, state-growth and candidate-artifact checks passed. ARM Rust
failed when a real-process restart test could not drain two remaining led groups
within 60 seconds. The failure is confirmed; its exact cause has not been
reproduced locally. The next changes tighten drain preflight and successor
eligibility and include group metrics and child logs in that failure diagnostic.
They require new remote checks, including ARM, before claiming resolution.

## Configuration-backed maintenance eligibility

Kubernetes readiness and metrics use a shared local Raft report whose expected
group IDs and voters come from static configuration. Missing groups cannot be
hidden by taking the union of observed metrics. Require a running, non-shutdown
group, completed recovery proof, no operator stop, uniform full voter membership,
no learners, local application through the effective membership entry, and a
known leader in the expected voter set. Bound local committed-to-applied lag by
the existing 16-entry maintenance tolerance. Operator permission to campaign
after majority loss is not equivalent to a completed recovery proof.

The CLI consumes this report and the per-group participation state before
mutating maintenance policy. Reject management endpoints reporting another
node's identity. Select only observed voters with open recovery gates, eligible
transfer policy and application through a captured committed prefix. Keep that
prefix fixed during a drain so continuous writes do not make the pre-transfer
eligibility target move indefinitely; the Raft transfer protocol still waits for
its recipient to flush the transfer request's log prefix before campaigning.
An unavailable successor is polled within the existing deadline, not nominated.

The retained 0.6.2 rolling-upgrade source lacks the additive report and uses
legacy checks until replaced. It cannot certify the repaired participation
contract. Remove that fallback once no supported upgrade source predates the
report. For new replicas, the rollout waits for the Ursula startup probe before
opening the management tunnel, repairs membership, waits for catch-up and
releases prepared-restart policy, then requires Ready and cluster verification.
Waiting for Ready before membership repair would deadlock with the stronger gate.

Local validation passed 834 workspace library/binary tests with one pre-existing
ignored stress test, workspace doc tests, all-targets Clippy, formatting and all
seven DST audits. All eight static-cluster entries passed locally (real S3
skipped without opt-in), plus rollout ordering/startup-probe shell regressions
and strict Helm lint. Focused cases cover a group missing from every observation,
incomplete promotion, closed recovery gates before any maintenance mutation,
wrong endpoint identity and continuous-write drain barriers.
The unchanged CI smoke-corpus/PR-seed, bounded-state and memory-WAL scripts also
passed locally on the maintenance changes.

This report is local eligibility, not a continuously refreshed quorum proof or
a cluster-wide maintenance reservation. It cannot serialize two independently
authorized disruptions, fence a lost host, or prove a replacement has exclusive
ownership of a Raft node identity. Those parts of milestone 3 remain pending;
direct Pod deletion also bypasses a Kubernetes PDB.

At `874c3a4226fd90fbf0cf4f81c15814049f9f7fa4`, remote amd64 and ARM Rust,
DST, lint, Helm, documentation, protocol, real-S3 integration, SQLite VFS E2E
and the state-growth ratchet passed. The former ARM drain timeout did not recur
in this run; that does not establish its exact cause. Both memory and disk soak
also passed; all scheduled checks completed successfully (the opt-in nightly
check was skipped).

The exact head was published as candidate `0.0.0-pr.874c3a4226fd` by
[run 37402662304](https://github.com/tonbo-io/ursula/actions/runs/37402662304):
image `sha256:4b11a1c8dbf166af21ff8f1a875237598b03c68ce8abb85e96d8afee288656aa`,
chart `sha256:96d36654ab1077c20678d91f5b10978bd0096852205e160b3193d1debeae950c`.
Publication is not deployment or EKS qualification.

The reviewed Cloud artifact entry point at
`c573431bfa4335d080d02ef1521fcf9c3f023dca` still fixes upgrade source
`967e71fd3f38` (Raft protocol 1), whereas this candidate uses format epoch 2.
Its fresh mode also compares the synthetic PR candidate version against the
deployed release line. Therefore that entry point must be repaired to qualify
a same-epoch 0.6.2-to-candidate upgrade before dispatching it; this epic does not
authorize cross-epoch migration. Cloud changes are isolated in
`/private/tmp/cloud-ursula-hardening`, branch
`fix/ursula-same-epoch-qualification`.

Cloud [PR #3174](https://github.com/tonbo-io/cloud/pull/3174) at
`c8dfa27f23e5366c27bfd48abd17015f21117e3a` replaces that fixture with the
declared canary release and immutable image/chart pins. It reads the actual
`FORMAT_EPOCH` literals at both source commits: equal epochs require upgrade;
fresh installation requires a newer epoch and release line. Synthetic PR tags
cannot select fresh installation. The client records each acknowledged payload
hash and offset independently and checks that journal after upgrade and again
after a complete sequential same-version restart. Six policy tests, ten
release-selection tests, five ACK-probe tests, source-format parsing, Python/Node
checks and shell/workflow checks passed locally. All required PR and merge-queue
checks passed, and the change merged as
`5d380186d1d285e61341bae158f7b4abf3e8069d`. The exact `874c3a4` candidate failed
the main-only EKS
[run 37405569442](https://github.com/tonbo-io/cloud/actions/runs/37405569442);
the namespace was read back absent after cleanup. This change does not convert
the 2-GiB inline fixture into production-config qualification.

That EKS run successfully initialized pinned 0.6.2 and wrote the independent
1,024-stream ACK journal. The first replacement passed its TCP startup probe,
but `repair-restarted-voter` immediately failed to resolve its client-plane DNS
name while fetching metrics. The hook's admin tunnel was already open; metrics
still preferred `http_url`. No post-upgrade ACK verification ran, so this is a
confirmed transport/control-path failure, not a passing RPO test or proof that
the core repair is qualified. The DNS failure is captured; the precise DNS cache
or publication timing has not been measured. Full logs and summary are retained
under `/private/tmp/ursula-hardening-eks-37405569442-evidence`.

The follow-up adds an explicit optional `metrics_url` to the CLI node manifest.
The chart hook sets it to the same Pod-bound admin tunnel and keeps `http_url`
as the advertised peer address used for learner attachment and Raft handoff.
Other manifests retain their client/admin fallback. A real local HTTP regression
uses an unresolvable advertised hostname, reads metrics through the tunnel and
checks that learner attachment still sends the original peer address. Shell
regressions check the tunnel/peer separation for every supported replica count.
These changes require new CI, immutable candidate publication and another EKS
upgrade run before merge or deployment.

At `9918c391a2a3c5aa61fedda74c28ad71a4f28888`, all scheduled remote checks,
including both 128-owner WAL soaks, passed (the opt-in nightly check was skipped).
Candidate publication [run 37406301991](https://github.com/tonbo-io/ursula/actions/runs/37406301991)
recorded image `sha256:93af1063d0aee1d0bbd5def06d83dead489c4a6579c77e8391a850dcea12148b`
and chart `sha256:9c5d0c71b11749982cb1e777bb21c8cff129cb346061740c507ceffcd1659e7c`.
The same-epoch EKS upgrade [run 37406598319](https://github.com/tonbo-io/cloud/actions/runs/37406598319)
passed the former metrics/DNS failure point but failed during the first voter's
repair. The independent journal again recorded 1,024 ACKed streams, but no
post-upgrade verification ran; cleanup read back the namespace absent.

Two mixed-version defects were identified. First, the old HTTP/gRPC mux routes
an unknown `RejoinBarrier` POST to the HTTP append handler and returns HTTP 400
`InvalidBucketId`, which tonic maps to `Internal`, not `Unimplemented`. The
new voter's fresh recovery gate therefore remained closed. Second, the old
leader's automatic detach/attach/promote repair can complete between CLI polls,
so the CLI repeatedly undid that repair while waiting to observe the detached
intermediate membership. Live group 15 metrics showed all three voters at the
same committed/applied index with the new target's recovery gate still closed;
the hook repeatedly issued successful detach operations until its deadline.
Evidence is retained under `/private/tmp/ursula-hardening-eks-37406598319-evidence`
and the corresponding `node-*-metrics.json` and legacy RPC response captures.

The follow-up negotiates barrier support through additive metadata on the
existing low-term Vote RPC, before sending any new method. Missing metadata
selects the legacy linearizable HEAD plus subsequent Vote proof; capability
metadata alone cannot open the gate and is not cached across replacements.
The real TCP regression models the legacy mux's HTTP 400 response and checks
that recovery never sends the unknown method, rejects missing fresh quorum
proof and still requires applying the proven prefix. CLI reconciliation accepts
a concurrent completed heal only after every configured node reports the full
uniform voter set, no learners, target apply through the observed committed
prefix, and participation readiness. A caught-up full set with a closed gate
waits without repeatedly detaching. New regressions cover automatic promotion
after both detach and attach, an unopened gate, and full membership without
target catch-up. This follow-up needs its own CI and immutable EKS candidate;
neither failed upgrade establishes an RPO pass or a recovery-time baseline.

The mux/concurrent-heal follow-up passed 839 workspace library/binary tests
with one pre-existing ignored stress test, workspace documentation tests,
all-targets Clippy, formatting, all seven DST audits and the madsim smoke
corpus. All eight real-process cluster entries passed (the real-S3 opt-in entry
skipped locally). The unchanged stronger rollout/Helm regressions passed on
the previous head. The new CLI regression also checks the supported 0.6.2
survivors' absent maintenance fields while retaining the upgraded target's
actual recovery-gate check. Remote and EKS validation of this follow-up remain
pending until its exact source and artifacts are recorded.

The transport follow-up passed 836 workspace library/binary tests with one
pre-existing ignored stress test, workspace doc tests, all-targets Clippy,
formatting and all seven DST audits. All eight static-cluster entries passed
(the real-S3 entry skipped without opt-in); the madsim smoke corpus passed as
well. CLI coverage includes successful metrics access with an unresolvable peer
hostname and the unchanged learner address on the wire. Hook ordering, manifest
and startup-probe shell regressions, shellcheck and strict Helm lint passed.

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

Read-only canary inspection confirmed three ARM64 shared-services voters,
OnDelete, 3-GiB memory limits, 1-GiB memory requests, zone anti-affinity,
256 groups, memory WAL and S3 snapshot/cold backends. The observed rollout state
was schema 2, complete, node 0, and every voter was Pod Ready with zero container
restarts. These old-server observations do not prove the new Raft readiness
contract, current Raft health, RPO or either recovery-time baseline. The generic
local `canary` context was stale; the confirmed context was
`tonbo-canary-cilium-1-eks`. Raw read-only captures are under
`/private/tmp/ursula-hardening-live-*.json`.

The existing Cloud artifact workflow provides a disposable namespace and cleanup evidence, but its candidate fixture uses 2 GiB, inline snapshots, disabled cold storage, and no gateway or indexer. It is suitable for its upgrade regression, not the selected production-config recovery baseline. Extend reviewed test automation with a separate production-config fixture, isolated S3 prefixes, concurrent append/read measurement and full-group redundancy sampling before reporting qualification results. Do not relabel the current local six-group tests or that candidate fixture as production-scale measurements.

Cloud main subsequently advanced to `b54d4b92261aefc950c6df4a0238c1402101aa10`
with [PR #3169](https://github.com/tonbo-io/cloud/pull/3169): declared per-zone
`r7g.xlarge` voter pools with a NoSchedule taint and MINIMAL node updates. Native
node auto repair remains disabled; the new managed-node-roll gates are read-only
pre/post checks, not fenced automatic recovery or a shared maintenance reservation.
This is source evidence, not confirmation that voter placement has changed live.
Refresh actual placement and resource limits before fixing the baseline fixture.

## Baseline measurement procedure

Keep the deployed 0.6.2 baseline and repaired candidate as separately pinned test
runs with the same production-derived configuration and workload. Record their
image/chart/source digests, effective configuration, voter Pod UIDs, host IDs,
zone placement and test-data prefixes. The test uses its own namespace, S3
prefixes and scoped role; a namespace alone cannot isolate a production S3 root.
Do not reuse the production storage ServiceAccount's authority. Reuse the
existing reviewed artifact workflow and EKS/OIDC infrastructure for execution.

Drive continuous append/read through the dedicated gateway while sampling all
configured groups. Store request start/end, actual response/ACK offset, producer
identity/sequence and payload hash outside voter state. Include a timestamped
fault boundary and keep every ACK inventory through recovery. Report unconfirmed
requests separately from confirmed writes; verify the confirmed journal with
payloads, offsets and producer replay results after repair. The load generator
must preserve its original producer identity and sequence across ambiguous
responses rather than inventing new writes during retries.

Measure one planned restart and one abrupt single-voter state loss after proving
that every group has three recovered voters. Begin each fault only after the
previous fault has regained full redundancy. A host-loss drill requires an
isolated, fenced test host and reviewed node lifecycle automation; deleting a
Pod on a healthy shared host is a different fault and cannot stand in for it.
Snapshot/S3 degradation is a separate run with the same single-loss boundary.
Record data volume, append rate and survivor/replacement peak memory for every
run before comparing the results.

Write recovery is measured from the fault boundary to the first successful
post-fault append, with subsequent failure gaps and end-to-end latency reported
separately so one lucky ACK cannot hide unstable service. Full redundancy is
measured to the first observation in which every configured group has the full
uniform voter set, completed participation gates, application through a captured
committed prefix and fresh quorum confirmation. Capture that prefix once per
verification round so continuous writes do not create an unreachable moving
target. Pod Ready, a union of observed group IDs, or an aggregate leader count
alone cannot establish recovery. Keep per-group evidence and sampling intervals
with both measurements; no numeric RTO acceptance limit is set before these
baselines are available.
