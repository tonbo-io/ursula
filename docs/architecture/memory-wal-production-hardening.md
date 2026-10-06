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

## Updated continuity scope

Owner clarification on 2026-10-06: qualify graceful Ursula upgrades using a general Durable Streams read/write workload. Agent Turn execution, Cloud session capture, billing settlement and Cloud application changes are not acceptance dependencies for this epic. Earlier Turn references below are retained as historical evidence, not outstanding release gates. Existing isolated test infrastructure and completed Cloud-hosted evidence remain reusable; they do not add Cloud product work to the scope.

Owner priority clarification: temporary 502/503 responses during upgrade are acceptable, and callers may retry using the documented protocol. Fully transparent upgrade behavior, automatic client retries/reconnection and availability-oriented gateway optimization are independent low-priority follow-up, excluded from epic completion criteria. Qualification still uses sustained generic Durable Streams load and an independent acknowledgement journal: retain every acknowledged append, preserve offsets and ordering, and resolve ambiguous appends using unchanged producer coordinates without duplicate logical appends. Read/subscription clients may reconnect from confirmed offsets. Report raw errors, retry counts, write interruption and full-redundancy recovery separately; retrying test load validates integrity and recovery, not a zero-error availability promise.

Single-voter local-state loss, exclusive maintenance/recovery gates, bounded memory, Raft telemetry and the correctness/resource-bound qualification matrix remain in scope. Throughput/latency optimization, faster rebuild tuning and same-topology memory/disk comparative benchmarks are independent low-priority follow-up, excluded from epic completion criteria. Existing performance observations remain evidence; no performance improvement or comparative benchmark is required to complete this epic.

## Milestones

| Order | Work | Acceptance evidence | Status |
| --- | --- | --- | --- |
| 1 | Fix early vote eligibility after memory-WAL state loss | Regression reproducing stale AppendEntries plus one state loss; acknowledged tail survives; real gRPC coverage; explanation of the recovery barrier | Merged in #370; local/remote CI and isolated EKS upgrade/restart passed at 4012d51; production qualification/deployment pending |
| 2 | Make snapshot reference publication failures recoverable | Temporary reference PUT failure followed by successful retry without restarting the Raft group; retain snapshot GC protection | Merged in #370; local/remote CI and isolated EKS upgrade/restart passed at 4012d51; production qualification/deployment pending |
| 3 | Raft-aware maintenance gates and automatic fenced replacement | Refuse a second planned disruption until every affected group regains healthy membership and catches up; replace dead-node voters without duplicate node identities | Shared planned Pod reservation/consumer merged in #376/#377 and qualified under production-config load; legacy managed-node guard merged in Cloud #3187; common provider admission and automatic fenced host replacement pending |
| 4 | Node memory budget and backpressure | Bounded retained/uncommitted logs, hot data, request queues and concurrent rebuilds under production limits and S3 degradation; reject load before OOM | Pending |
| 5 | Raft observability | Metrics and alerts for quorum risk, stopped groups and stalled recovery; synthetic append/read; preserve clear retryable failure behavior | Pending |
| 6 | Production-scale qualification | Isolated fault injection with production topology/workload; measured write-recovery and full-redundancy RTO; RPO zero inside the failure model; generic Durable Streams integrity and recovery during sequential upgrades with caller retries; record temporary failures and verify memory/queue bounds | Normal-Pod baseline/candidates and #377 complete serial Pod maintenance qualified; abrupt-host, retained-volume and resource-stress qualification pending; numeric RTO targets remain unset |
| Supporting | Accurate chart/config durability docs and bounded SIGTERM handoff | Chart rendering; config documentation matches abort behavior; real-process handoff and bounded no-quorum shutdown | Merged in #370; local/remote CI and isolated EKS upgrade/restart passed at 4012d51; production qualification/deployment pending |

## Revised goal objective

Complete Ursula memory-WAL production safety hardening for the fixed three-voter topology: tolerate one unrecovered voter losing all local state while preserving acknowledged data and protocol state; fix the reproduced recovery-barrier and snapshot-publication defects; implement exclusive Raft-aware maintenance admission and fenced automatic single-voter replacement; bound node memory, queues and rebuild concurrency with backpressure; provide necessary Raft metrics and recovery alerts; and qualify the reviewed release artifacts with generic Durable Streams workloads in an isolated production-scale environment. Preserve majority-loss fail-closed/manual data-loss acceptance, accurate durability documentation and bounded planned shutdown. Keep source, CI, artifact and live-qualification evidence distinct. Temporary 502/503 responses and protocol-safe caller retries are acceptable. Exclude performance optimization/comparative benchmarks, transparent zero-error upgrade behavior, Agent Turn tests and Cloud business integration from goal completion. Production remains read-only during qualification. The owner resumed the goal with this revised scope on 2026-10-06.

This owner-revised objective supersedes older goal text mentioning business retries or Turn continuity. Historical tool metadata is not an additional completion requirement.

## Epic completion criteria

Complete the core correctness fixes; qualify acknowledged-write and protocol-state preservation after one unrecovered voter state loss; implement exclusive maintenance admission and fenced automatic replacement with full-group recovery before the next disruption; verify node memory/queue bounds and backpressure under sustained load and storage degradation; provide necessary Raft health metrics/alerts; and pass the corresponding isolated qualification at the declared production scale. Record write-recovery/full-redundancy timing and resource limits. Correctness, recovery completion and resource-bound tests remain required; making measured recovery faster or improving throughput/P99 does not.

## Deferred outside epic completion

- Performance optimization and comparative benchmarking: throughput/P99 improvements, rebuild acceleration and memory/disk WAL performance comparison.
- Transparent upgrade availability: automatic client/subscription recovery, gateway availability/retry optimization and a zero-visible-error upgrade experience. Temporary 502/503 responses and protocol-safe caller retries are acceptable.

Neither deferred item is a prerequisite for declaring this epic complete. Agent Turn and Cloud business integration also remain excluded.

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

The core follow-up at `4012d5171a134beacb0d3a941019adb25252a5eb` passed all
scheduled remote checks, including both 128-owner WAL soaks (opt-in nightly
skipped). Candidate publication [run 37407902210](https://github.com/tonbo-io/ursula/actions/runs/37407902210)
recorded image `sha256:2f3fa52cf1510c6feedaed7edff7c3c51910f5302c46ebb3c711c637ca82b0a0`
and chart `sha256:4af5d1269d6420ede55dc8842c7b23a3bf46a540536a45e8584b8ca0e4a4468d`.
Same-epoch EKS [run 37408145007](https://github.com/tonbo-io/cloud/actions/runs/37408145007)
passed the 0.6.2-to-candidate upgrade and a complete sequential same-version
restart. The independent journal verified the same 1,024 ACKed payload hashes
and offsets after both passes; every replacement passed strict 256-group
readiness. The summary records exit status 0 and namespace deletion, confirmed
by a separate read-only query. Evidence is retained under
`/private/tmp/ursula-hardening-eks-37408145007-evidence`. PR #370 merged as
`60c8d82bd2fbe9ea7785f283e764ed197c432b2f`; its Git tree exactly matches the
qualified source tree. This accepts the core and upgrade regression milestone,
not production-config recovery times, S3 fault recovery at production volume,
Turn continuity or production deployment.

Use a dedicated test cell reusing production configuration for fault injection
(user selected). Existing production cells are read-only sampling targets until a concrete
reviewed test procedure authorizes disruptions. Reuse the production three-voter,
256-group configuration and freeze its current memory/placement settings; confirm actual cold
storage, placement, data volume, write rate and workload before treating it as equivalent.

Record separate targets and measured distributions for:

- time until successful writes resume;
- time until every group has a caught-up full voter set;
- generic Durable Streams integrity after protocol-safe caller retries and read reconnection; record errors and retry time without requiring transparent recovery;
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

Cloud main then advanced to `fe9bbe1c74dbfa7de55df6850a90229ab3318632` through
[PR #3170](https://github.com/tonbo-io/cloud/pull/3170), declaring dedicated
ARM64 voter placement and matching 16-GiB requests/limits. Read-only canary
sampling during migration confirmed the StatefulSet's new desired settings,
two actual 16-GiB voters and one remaining 3-GiB voter, all still on 0.6.2.
The raw capture is `/private/tmp/ursula-hardening-prod-placement-20261006.json`.
Subsequent read-only inspection confirmed the graceful-rollout Job terminally
failed with `BackoffLimitExceeded` at `2026-10-06T03:04:31Z`. Its log records
the old hook's metrics DNS lookup failure after replacing node 2; this is the
known tunnel-selection defect repaired in PR #370, not an ongoing migration.
Evidence: `/private/tmp/ursula-hardening-prod-baseline-freeze-status.json` and
`/private/tmp/ursula-hardening-prod-migration-failure.log`. Freeze the declared
16-GiB target as the qualification configuration and record this mixed live
state separately; it is not a steady-state recovery baseline.
Capacity changes do not establish bounded memory, exclusive replacement
identity, automatic dead-node recovery or either RTO baseline.

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

## Qualification infrastructure and fresh-prefix preparation

Cloud [PR #3175](https://github.com/tonbo-io/cloud/pull/3175) merged at
`8e1f017df49ebc0449238c7e683a016bcd887501` after its PR and merge-queue checks.
It declares a canary-only three-zone qualification pool derived from production
voter instance types and zones, a separate namespace and exact IRSA subjects,
and disjoint server/index test S3 prefixes. Reviewed-main IAM
[plan 37412094352](https://github.com/tonbo-io/cloud/actions/runs/37412094352) and
[apply 37412248380](https://github.com/tonbo-io/cloud/actions/runs/37412248380)
succeeded: four resources added, none changed or destroyed. Serving roles,
storage and voters were untouched.

The first targeted compute
[plan 37412362067](https://github.com/tonbo-io/cloud/actions/runs/37412362067)
failed before apply: the qualification contract supplied the AWS spelling
`NO_SCHEDULE`, while the reused managed pool expects Kubernetes `NoSchedule`
and maps it to the AWS enum at the resource boundary. Cloud
[PR #3176](https://github.com/tonbo-io/cloud/pull/3176) corrects that contract and
adds a regression and merged as `d90a8fbaebde934a958055fd526b926bd55edc59`;
its reviewed-main compute plans still need to be inspected before capacity creation. No qualification node has been created by this failed
plan.

Local `ursulactl verify-quorum` implementation captures a fresh outbound
ReadIndex prefix for every explicitly configured group and waits for every
configured replica to apply that fixed prefix under the same leader term.
It reuses the memory-WAL recovery probe, validates complete uniform membership
and local participation reports, and bounds metrics requests, RPCs and waits by
one absolute deadline. An explicit 0.6.2 diagnostic option returns
`participation_certified=false`; missing old fields cannot certify the repaired
participation guarantee. The shared probe now also refuses a vote/term change
between its routing probe and ReadIndex result. Local validation passed 846
workspace unit/bin tests, workspace doc tests and all-target Clippy, all seven
DST audits and the smoke corpus, and all eight real-process static-cluster
integration tests. The focused real TCP/gRPC fixture checks six groups on all
three replicas and rejects a configured seventh missing group. This is observation evidence,
not a maintenance reservation or physical fencing authorization. Full M3 still
requires cross-workflow serialization and incarnation-aware replacement.

The production-config value derivation and continuous append/read probe are
also implemented locally in Cloud, with immutable server image pins, run-scoped
roots, preserved producer identity/sequence/body across ambiguous retries, an
independent ACK event journal, full acknowledged-prefix verification and
replay deduplication. Native Helm rendering of the pinned 0.6.2 baseline passed
the isolation guard. Their reviewed EKS orchestration, fault boundaries and
final run evidence remain pending; no production-scale RTO or Turn result is
claimed by these preparation steps.

## Fresh-prefix qualification and dedicated-cell capacity

[Ursula PR #371](https://github.com/tonbo-io/ursula/pull/371) merged as `f4f479ba35eed3d397554f94fb1ed2f06182edc1`. Its Git tree `0fd388ceadaa96b14c8de36dcf2848f663f299cc` exactly matches qualified source `92e40d8d11f465f2645804a2e5e2433e3329673a`. All scheduled remote checks and both WAL soaks passed (opt-in nightly skipped). Candidate [publication 37413986456](https://github.com/tonbo-io/ursula/actions/runs/37413986456) recorded version `0.0.0-pr.92e40d8d11f4`, image `sha256:4f89661465530e7a14cee522c11425947800eb1cae9fc33145a1b19e5472ce98` and chart `sha256:b593711c3e7203708ec5a39fc37ae543488d0b6091f381fd2925b950e013bcd8`.

Artifact [EKS run 37414121179](https://github.com/tonbo-io/cloud/actions/runs/37414121179) passed pinned 0.6.2 upgrade and complete sequential restart. All 1,024 ACKed payloads and offsets matched after both passes, with payload hash `d86a5a64a5450bdcf78a3f19c8d0f962855938623c675a01e20a8f0d929d5252`. Its summary records exit status 0 and namespace deletion, separately verified read-only. Evidence is `/private/tmp/ursula-fresh-quorum-eks-37414121179`. This accepts implementation/upgrade regression, not production-config RTO, automatic host recovery or Turn continuity.

Cloud taint repair merged as `d90a8fbaebde934a958055fd526b926bd55edc59`. Main compute plans [37413470038](https://github.com/tonbo-io/cloud/actions/runs/37413470038), [37413953473](https://github.com/tonbo-io/cloud/actions/runs/37413953473) and [37414372883](https://github.com/tonbo-io/cloud/actions/runs/37414372883) were inspected before separate applies [37413548661](https://github.com/tonbo-io/cloud/actions/runs/37413548661), [37414105824](https://github.com/tonbo-io/cloud/actions/runs/37414105824) and [37414455994](https://github.com/tonbo-io/cloud/actions/runs/37414455994). The first plan created 13 qualification-only resources including one node group and all three groups' supporting roles/templates; the others each created one node group. None changed or destroyed serving resources. Read-only inspection confirmed three Ready `r7g.xlarge` test hosts in the three source zones; capture is `/private/tmp/ursula-qualification-hosts-created-20261006.json`.

Cloud [PR #3177](https://github.com/tonbo-io/cloud/pull/3177), source `50cfe30d5d6ad32bb58e699c844474bccb4f31fa`, extends the existing artifact workflow with production-config baseline/candidate observations. It derives ordered serving values and uses isolated identities/roots/hosts; one UID-bound normal Pod termination runs under continuous gateway append/read with an external ACK journal. Actual container incarnations, clock uncertainty, all-group fresh prefixes, full acknowledged-prefix/tail/dedup proofs and storage isolation are recorded. Cleanup stops all test writers before exact-root deletion and reads storage and namespace back absent; predecessor takeover is refused. Full local Python validation passed 2,005 tests; final focused checks include 25 qualification tests and four Node tests. Cloud #3177 merged as `e30bd2209dc85792895818474f3517833b7341ee`, with the same tree as its tested source. The first production-config baseline [37415939149](https://github.com/tonbo-io/cloud/actions/runs/37415939149) refused the versioned S3 bucket before creating a namespace or injecting a fault. It supplies no recovery-time evidence; Cloud #3178 merged as `423f8c477dacbdd44f4572a78c83e1e52ba43cd9`, adding explicit version/marker/multipart cleanup under an exact-run destructive session policy before rerunning. This synthetic first experiment does not establish production data volume, abrupt/fenced host-loss recovery, exhaustive per-group client availability or Turn continuity.

## Two-survivor proof for fenced recovery

[Ursula PR #372](https://github.com/tonbo-io/ursula/pull/372), merged as `0ddbfcfce36e810d05d7f1c1f52d958a7c4e83e2`, adds a separately scoped `verify-survivors` observation. It requires the original three-voter manifest and one explicit exclusion; both observed replicas must retain the complete uniform membership, exact group inventory and participation gates, confirm fresh prefixes, and apply them under the same leader term. The output always states `full_redundancy_restored=false`. It never authorizes a second disruption or establishes that the excluded host is fenced. Complete `verify-quorum`, exclusive maintenance reservation and physical incarnation fencing remain separate required gates.

Local validation passed 64 CLI unit tests and a six-group real-TCP fixture: stopping one voter makes full verification fail while the two-survivor proof succeeds; stopping a second makes survivor verification fail. Workspace formatting, all-target Clippy, lib/bin tests and documentation tests passed. All scheduled remote checks and both 128-owner WAL soaks passed. [Publication 37417260441](https://github.com/tonbo-io/ursula/actions/runs/37417260441) pinned source `8b43b9080b511fa841a3e989db9b2522fd90a297`, image `sha256:246b39db2d6526fd52d1be04af8c6766667c46ccd5e1b32feb6f3e9c64230f3d` and chart `sha256:932885baeb11114103b325e5f68f30f75f4f732a52b372cd6454c4a1d20baab8`. Its tree `d473226104d33a214cda9431d39e34bb85663c5b` matches the merge. [EKS regression 37417715169](https://github.com/tonbo-io/cloud/actions/runs/37417715169) passed 0.6.2 upgrade/sequential restart with all 1,024 ACKed payloads/offsets and separately verified namespace absence. This remains the 2-GiB inline fixture. Read-only survivor probes correctly refused maintenance-closed gates during the hook and a leader outside the selected survivors afterward; positive single-loss survivor evidence remains the six-group TCP fixture, not a 256-group host-loss qualification.

## Production-config fixture failures before fault injection

[Baseline run 37418152205](https://github.com/tonbo-io/cloud/actions/runs/37418152205), from Cloud main `423f8c477dacbdd44f4572a78c83e1e52ba43cd9`, installed pinned 0.6.2 with the declared 16-GiB production configuration. Actual three-zone hosts and positive/negative IRSA checks passed; the initial fresh-prefix observation covered 256 groups and all three replicas, with legacy participation explicitly uncertified. Its projected ConfigMap symlink made the Node entrypoint comparison silently skip the workload. No fault was injected and no RTO was measured. Cloud #3179 merged as `08aea1d1575634ac672d8661aee00cdf69b74554`, resolving the entrypoint and requiring exact initialized inventory plus a live client before fault admission; a native process/HTTP test reproduces the bug on the old main source and verifies full ACK/prefix execution with the fix.

All test Pods stopped, but S3 cleanup failed parsing a response and retained the namespace and remaining historical versions. Quiet-mode deletion may return empty CLI output; Cloud #3180 adds support for that documented response boundary and exact-predecessor cleanup recovery. The retained namespace UID `17fbcc6d-aa4f-49f0-8c9f-4d5d2d90f391` and run label `37418152205-1` were captured read-only. Thirty-seven focused Python checks and authorization against the real downloaded run evidence passed. Cloud #3180 merged as `33272e2060669aac009137a61f1a824b8ab259be`, with the same tested tree. Reviewed-main cleanup [37420539155](https://github.com/tonbo-io/cloud/actions/runs/37420539155) removed 26 server/four indexer historical versions and two delete markers, read both roots empty, and deleted the original namespace UID. Independent namespace/version/multipart reads confirmed absence. Cloud #3181 merged as `64c3c1f509fbd51ac57537bebc1d0500fe8e5cab`, removing that sole pre-receipt compatibility consumer; future cleanup requires a recorded namespace UID. Failure evidence is `/private/tmp/ursula-production-baseline-37418152205-evidence`. Serving cells were not mutated.

## Memory-budget preparation: static findings

The rendered production-config fixture sets a 64-MiB hot-data admission cap per group and 256 groups: their permitted total alone is 16 GiB, equal to the voter container limit, before log, state metadata, request bodies, caches or rebuild buffers. This arithmetic is a configuration review, not an observed RSS result. The source already has a node-wide applied-log snapshot-pressure gate and bounded snapshot build/install concurrency; these are useful controls but do not establish an aggregate node memory bound. The rendered uncommitted-byte setting is disabled. M4 must account for existing controls and reserve node-wide memory before accepting work, rather than treating a larger container or the RSS abort cap as backpressure. The runtime admission module comment is corrected to distinguish the fail-stop RSS cap from HTTP 503 admission.

## First production-config planned-termination baseline

[Baseline 37420679131](https://github.com/tonbo-io/cloud/actions/runs/37420679131), Cloud main `33272e2060669aac009137a61f1a824b8ab259be`, passed with pinned 0.6.2 and the frozen declared production configuration. The 1,024-stream workload retained and verified all 12,845 acknowledged 4-KiB appends (52,613,120 bytes), including final complete prefixes, exact tails and duplicate replay. There were 224 append 503s, 12 append 502s and 108 read-after-ACK 503s, all retried with unchanged operation identities. Continuous throughput was 136.28 appends/s. Maximum append operation latency including retries was 6,455 ms; maximum aggregate ACK gap was 3,820 ms.

The first definitely post-request ACK fell 32–610 ms after the UID-bound normal deletion request, reflecting surviving groups rather than complete write recovery. Maximum per-stream recovery ACK gap was 15,448 ms (P50 13,565/P95 15,384/P99 15,438 ms), versus pre-fault maximum 7,011 ms. Because 16 workers rotate through 1,024 streams, these gaps include normal scheduling delay; they are not pure server-unavailability measurements. All streams had a post-request ACK; the largest first-ACK upper interval endpoint was 15,434 ms. Clock offset was bounded by −20 to +544 ms.

The first successful 256-group/three-replica fresh-prefix observation started 13,625 ms after delete completion and finished 15,341 ms after delete start. Completion establishes a 15.341-second full-redundancy recovery upper bound for this run; observation start is not a lower bound on the actual repair instant. Legacy participation remained explicitly uncertified. The final proof also passed. Cleanup removed 2,404 server/two indexer versions and both markers, and independently read the namespace and both exact roots absent. Evidence is `/private/tmp/ursula-production-baseline-37420679131-evidence/ursula-artifact-37420679131-1`.

This single normal-Pod-termination synthetic baseline does not qualify abrupt host loss, production retained volume, all-group client availability or Turn continuity, and establishes no numeric acceptance limit. The matched candidate [37421045806](https://github.com/tonbo-io/cloud/actions/runs/37421045806), source `92e40d8d11f465f2645804a2e5e2433e3329673a`, passed with the same Cloud revision, tools, declared configuration and workload. All 13,472 ACKed 4-KiB appends (55,181,312 bytes) were verified. Initial, pre-fault, repaired and final proofs each covered all 256 groups on all three replicas with participation certified. Maximum append latency including retries was 5,623 ms; maximum aggregate ACK gap was 4,315 ms. Full proof started 14,160 ms after delete completion and finished 16,330 ms after delete start. Per-stream maximum recovery gap was 14,809 ms versus pre-fault maximum 6,970 ms. There were 185 append 503s, 14 append 502s and 70 read-after-ACK 503s. Continuous throughput was 138.18 appends/s. Cleanup and independent namespace/current-version-multipart absence checks passed. Evidence is `/private/tmp/ursula-production-candidate-37421045806-evidence/ursula-artifact-37421045806-1`. One sample per version cannot establish a performance improvement or tail-latency acceptance limit; both retain transient 502/503s, acceptable with protocol-safe caller retries under the revised scope.

## Pod deletion identity boundary

The rollout helper recorded a source Pod UID but refreshed its target by name and issued an unconditional named deletion. A concurrent replacement could therefore be deleted by a stale caller. The helper now requires the admitted source UID, sends Kubernetes `DeleteOptions.preconditions.uid` through the existing kubectl transport and stops on a conflict, preserving configured termination grace. Each caller passes its recorded/observed source incarnation; a replacement does not refresh authority by name. Shell regression checks accepted deletion, stale-UID refusal and invalid-identity refusal; it fails on pinned pre-fix source `0ddbfcfce36e810d05d7f1c1f52d958a7c4e83e2`. The existing remote disposable-cluster upgrade test additionally attempts a second deletion using the retired UID and requires the replacement to remain undeleted. [Ursula #373](https://github.com/tonbo-io/ursula/pull/373) merged as `8558e7d1b36355df58294cc48a20d831e62ed86e`, with identical tested tree `ef633ac3efe9dc36deedd32394a010daaa04c952`. CI [37421631806](https://github.com/tonbo-io/ursula/actions/runs/37421631806) passed actual API conflict rejection and retained the replacement Pod; all scheduled checks and both 128-owner WAL soaks passed. [Publication 37421993006](https://github.com/tonbo-io/ursula/actions/runs/37421993006) pinned source `38228875e1fba3760dc50eb3da447d0767ac1eb2`, image `sha256:42839936312c3ff51947bc1f73c38002019b50169126e9609e7614ae4f7b79a5` and chart `sha256:82c312313e2ba1e931a11ce3d57db29ede90f7bdb3caeaaab080dd56ff0d8e41`. [EKS 37422131622](https://github.com/tonbo-io/cloud/actions/runs/37422131622) passed 0.6.2 upgrade and sequential restart with all 1,024 ACKed payloads/offsets retained; namespace absence was independently confirmed. This remains the 2-GiB inline fixture. Cloud #3182 merged as `9b42d4364358d9516098efdc668c043a07d4d5a4`, applying the same UID precondition to the superseded-rollout recovery plan. Its source-pinned conflict regression, local checks and all PR/merge-queue checks passed. The serving recovery workflow was not dispatched. This boundary does not serialize workflows, bind admin commands to a process boot ID or establish physical host fencing; those M3 gates remain required.

## Next M3 boundary: process and reservation identities

Pod deletion preconditions do not identify an Ursula process restarted within the same Pod. The next implementation must expose a fresh server-instance identity, pin it before a maintenance plan sends mutations and reject missing or mismatched identities on current admin mutation routes. One CLI invocation must not refresh its pinned identity after a failure; durable plans must retain those identities across invocations. The election handoff and backup-import mutation paths require the same audit as drain, quiesce and membership changes. A mismatch must leave the operation failed and require a new reviewed observation, rather than silently acting on the replacement.

A cell-wide CAS maintenance reservation must persist across executor failure, bind the selected Pod/process/Node/provider instance and deny expiry-based takeover. Completing a replacement requires current-incarnation all-replica fresh-prefix certification before releasing authority for the next disruption. Host-loss recovery additionally requires evidence that the old provider instance is irreversibly terminated before force-removing its Kubernetes identity; Pod or Node API deletion alone does not establish physical fencing. These gates must be reused by chart rollout, Cloud managed-node updates and recovery. The qualified process guards below complete that instance boundary; shared reservation and host recovery remain required.


## Qualified process-incarnation guards

The server publishes a fresh canonical process identity in metrics and requires that identity on all admin-plane mutations (`428` missing, `412` changed), before handlers execute. CLI clones preserve the first observation and explicit manifests bind identities across separate invocations. Current self-election uses the guarded admin route, backup imports retain the shared pin, and failed identity preconditions do not refresh authority or fall back to legacy consensus RPCs. Deployed through-0.6.2 servers and retained schema-1/2 rollout states are named migration consumers; their missing process identity remains uncertified.

The chart stores the manifest in rollout state schema 3. Initial admission pins every serving voter; an admitted source Pod replacement may update only the target process while both survivors must match. Once the replacement Pod UID and process are recorded, a resumed Job validates that process; a container restart within the same Pod or another Pod replacement fails closed. This is an instance precondition, not a shared maintenance reservation, provider-host fence, or complete M3 implementation. The no-identity drain regression fails on pinned main `8558e7d1b36355df58294cc48a20d831e62ed86e` (200 instead of 428). Local workspace units/binaries, documentation tests, all-target Clippy, the eight-test native process integration suite (the S3 case is environment-gated locally), the documentation-site build, 32 Helm template tests, 26 chaos tests, rollout regressions and seven DST audits passed. The real restart integration test proves a retired process cannot clear its replacement drain and requires explicit one-voter rebinding before repair. [PR #374](https://github.com/tonbo-io/ursula/pull/374) merged as `17e22bf795c7bcd3841363a284660d19e43450f8`. Its tree `5951104e3ec71c31fde6e93519f393e1b1f9bebd` exactly matches qualified source `0a7e5ebade56fe2ada697180f7af8f29b215b415`; no serving deployment is claimed.

The first isolated upgrade [37425648043](https://github.com/tonbo-io/cloud/actions/runs/37425648043), source `f2f9cff5a3faecd3f8ad6fc9a21efacebd98cc24`, rolled two voters but stopped repairing the third while automatic healing removed it and re-added it as a learner. Full-redundancy readiness correctly reported incomplete membership; using that same condition to enter single-target recovery incorrectly refused the healthy two survivors. The run failed, verified no final payloads, and cleaned its namespace; it supplies no successful upgrade or RTO evidence. A source-pinned regression fails on `d27fb42ae685f0b3373f9fd9334b1b0323d23d8a`. The recovery-only check now permits uniform membership containing the two original survivors, with only the selected target optionally a learner. It preserves all recovery-barrier, running, membership-applied, non-joint, inventory and catch-up conditions; disagreement, a missing survivor or another learner still blocks. Transient ineligibility waits within the existing drain retry deadline without transferring leadership or changing membership. Full-readiness certification remains unchanged and cannot authorize another disruption until all three replicas are restored. Three regressions cover this distinction, eventual convergence and timeout without repair mutations. An ARM failure also exposed an ambiguous startup-drain test. The final fixture waits for all memory recovery barriers, stops the two other Raft engines and observes the target for 15 seconds, including rejection of self-votes. Changing only `start_maintenance_drained=false` fails the same assertions (self-vote, term 6 versus 1); all 161 Ursula unit tests pass with the fence. Its predecessor failure log does not establish which peer initiated that earlier term advance.


Final-source [CI 37428343421](https://github.com/tonbo-io/ursula/actions/runs/37428343421), every scheduled PR check and both 128-owner WAL soaks passed. [Publication 37428344564](https://github.com/tonbo-io/ursula/actions/runs/37428344564) recorded version `0.0.0-pr.0a7e5ebade56`, image `sha256:06d42b7e9902a1a618401c30011f1dae7cdd66ace49f6527d6bd06a724a30cf7` and chart `sha256:4c41d48bd59b0ca7dc57f76a80f7884f98f900c2ab600438303e39b78d233f44`. [EKS 37428754696](https://github.com/tonbo-io/cloud/actions/runs/37428754696), Cloud `9b42d4364358d9516098efdc668c043a07d4d5a4`, passed 0.6.2 upgrade and complete sequential restart. Both readbacks preserved all 1,024 external ACK payloads/offsets, hash `3679a86eee8b57c82b4ca1b345a0ba70d01a6e2d062ceced96116f89973f591c`. Namespace absence was independently confirmed; evidence is `/private/tmp/ursula-incarnation-eks-0a-evidence`. This remains the 2-GiB inline-snapshot upgrade fixture.

[Production-config qualification 37429339626](https://github.com/tonbo-io/cloud/actions/runs/37429339626), Cloud `b84fe74bed991868e5003ac184487083827dcc25`, passed on that immutable candidate in the dedicated three-zone pool with 16-GiB voters, two cores, 256 groups, S3, gateway and indexer. Its three serving-value input hashes match the frozen source; the intervening Cloud #3183 changed only HelmRelease retry annotations and their runbook. All 14,189 acknowledged 4-KiB appends (58,118,144 bytes) across 1,024 streams were verified through complete prefixes, exact tails and duplicate replay. Initial, pre-fault, repaired and final proofs each covered all 256 groups on all three replicas, with process identities and Raft participation certified; only the selected voter changed process identity. Maximum append operation latency including retries was 5,622 ms, aggregate ACK gap 4,529 ms, and per-stream recovery ACK gap 15,105 ms versus pre-fault maximum 6,985 ms. Per-stream P50/P95/P99 observed maximum gaps were 13,311/14,835/15,091 ms; these include the round-robin workload delay and are not pure server unavailability. The test producer absorbed 201 append 503s, 17 append 502s, 107 read 503s and one read 502. Continuous throughput was 137.953 appends/s. The first successful full-prefix observation started 20,784 ms after delete completion and finished 22,672 ms after delete start, providing an upper bound rather than the exact repair instant. A single normal-Pod-termination experiment supplies neither a performance comparison nor numerical acceptance limits; abrupt/fenced host loss, retained production volume and memory-stress qualification remain open. Business retry integration and Turn continuity are excluded from this epic. Cleanup removed 2,991 server versions/73 markers and two indexer versions/one marker; independent namespace and exact-root version/multipart reads confirmed absence. Evidence is `/private/tmp/ursula-incarnation-production-0a-evidence`.


## M3 executor admission foundation

The next change adds process-local executor tokens, ordered activation/retirement and immutable CLI propagation. PR #375 merged as `c066d14823fc90dfa6c50e0f3d7d9e4af5c33466`; its tree `2d1bdc4e40875d7eefc42fe566d8bd9dfbae9ea0` exactly matches qualified source `19732a42e1e354b4063813fe22d4d1e6973538fc`. [CI](https://github.com/tonbo-io/ursula/actions/runs/37432113224), [real-S3 integration](https://github.com/tonbo-io/ursula/actions/runs/37432113361) and [both 128-owner WAL soaks](https://github.com/tonbo-io/ursula/actions/runs/37432113357) passed. The integrated shared reservation consumer remains unimplemented and unqualified; no serving deployment is claimed. The token comes from an external reviewed CAS reservation; the new protocol neither creates that reservation nor authorizes physical replacement. Activation must wait for admitted HTTP mutations and prior local Raft API messages. Cancelled callers must not release unfinished work, and cancelled or failed lifecycle transitions must retain their generation and close mutation admission. Retirement must never return to the uncertified unclaimed state. Submission ordering does not prove asynchronous replication or provider operations finished; fresh all-replica certification and exact physical lifecycle reconciliation remain required. Chart/Cloud shared CAS integration, automatic fenced host recovery, abrupt-host fault qualification and M4–M6 remain pending.


Local full-workspace library/binary tests passed (174 Ursula and 77 CLI tests), as did all-target Clippy, formatting, documentation tests, the documentation-site build, seven DST audits and the madsim smoke corpus. The eight native process integration cases passed; the S3 restart case is environment-gated locally. The memory-voter restart now retains its immutable token, rebinds only the replacement process, reads back six acknowledged payloads and obtains fresh six-group proofs before and after all processes retire the token. A separate three-node TCP fixture rejects certification of mixed active/retired executors. Cancellation tests cover an unfinished HTTP mutation, pending activation/retirement, unresolved-handler poisoning and the actual Raft API queue. None of these results establishes a shared reservation, provider-host fencing, production recovery time, or active-Turn continuity.


## Shared reservation policy implementation

The current `ursula-ctl` change supplies pure ownership/progress policy and offline whole-ConfigMap CAS proposal/acknowledgement commands. Planned Pod replacement requires a fresh all-active three-voter proof before admission, retirement of the original Pod UID, target-only process binding, and a fresh all-retired nonregressing proof before releasing the reservation. Takeover preserves the selected source and admitted write boundary, advances the executor generation and cannot choose another voter. Completed state retains the generation and replacement receipt. This is a foundation for the rollout consumer; it is not yet integrated into chart maintenance, qualified against a live Kubernetes CAS store, or sufficient for physical host recovery.

Eleven policy regressions and the actual CLI process test cover competing CAS proposals, fixed-source takeover, stale/incomplete proofs, same-UID refusal, target-only binding, completion and persistent JSON round trips. The complete CLI path caught and fixed serde's buffered enum parsing failure for numeric JSON map keys. The synthetic proof fixture is explicitly not live Raft evidence. Consumer integration, two-survivor host-loss admission and exact physical fencing remain outstanding. See `maintenance-reservation.md` for the current interface and boundaries.

## Shared-rollout qualification: stale follower cursor

The first shared-reservation EKS run
[37449534958](https://github.com/tonbo-io/cloud/actions/runs/37449534958),
Ursula source `db5d13997391e2a556e4e7c6fd7f459ea2a90d85`, completed the actual
three-voter Helm hook but failed its observer and did not complete final ACK
verification. Its journal also records a distinct read-after-ACK failure:
stream `qualification-recovery/stream-320`, acknowledged start offset 90112,
then HTTP 416 reporting tail 81920. This run does not establish data preservation
or qualification success. Namespace and exact S3 roots were independently read
absent after cleanup. Cloud #3186 repairs the observer list/watch handshake and
order-independent transition verification; it does not fix this read failure.

A real TCP/gRPC regression on core baseline
`14d89e378afe0e1cf4dd08eb9ae05b50e6f19223` pauses one follower's replication after
all three apply a ten-byte prefix. The other two voters acknowledge two more
appends through offset 18. Reading their acknowledged start offset 14 from the
lagging follower reproduces `OffsetOutOfRange`, reporting local tail 10. This
reproduces the stale-read mechanism without losing data; it does not prove that
the failed EKS run had no additional recovery defect.

Follower-local read-plan `OffsetOutOfRange` now joins the existing
`StreamNotFound` forwarding path; these forwarded boundary checks use a
quorum-confirmed leader read. Without a known leader, return the existing
leader-unknown retryable refusal. Preserve truly invalid cursor errors and the
owner-pinned live-read contract. The regression checks the exact acknowledged
payload/offset, genuine out-of-range refusal, refusal when quorum confirmation
fails, and the complete prefix on all three replicas after repair. Full reviewed
artifact and production-scale qualification remain required.


## Follow-up: survivor eligibility after restart leader pinning

Core candidate `9590730dc057100eaf8923d955baa669e2850811` passed every scheduled
remote check and both WAL resource soaks. Publication
[37451918285](https://github.com/tonbo-io/ursula/actions/runs/37451918285) produced
version `0.0.0-pr.9590730dc057`, image
`sha256:cec3b84abfa3c5b1a5e2bbb162f8b1100cbf0d3727368ecdc8430d503bacd578`
and chart
`sha256:32158a2c461ada6acf2f964bb71f6ed0cae84bc5d2c7a2cbc0dccb38154ffc48`.
[Isolated EKS 37452468568](https://github.com/tonbo-io/cloud/actions/runs/37452468568)
completed its 0.6.2 upgrade and independently verified all 1024 acknowledged
payloads/offsets, hash
`48314800c159329e0d6747e9504de6c5dcdae1719b8e73794b26795dd89dadf0`.
The same-version restart failed during the last selected voter's repair: after
successfully pinning leaders, a one-shot survivor check reported voter 3 not
complete/caught up. No final readback ran. Namespace absence was independently
verified. The old aggregate diagnostic does not establish which group or
eligibility condition failed; do not claim an exact live root cause or full
qualification. Initial dispatch 37452374099 supplied mutually exclusive PR and
version inputs and was refused before credentials/test resource creation.

A synthetic HTTP regression against the same CLI baseline reproduces the
one-shot failure when a survivor briefly reports joint membership immediately
after successful leader pinning. Recovery now waits read-only, within the
existing drain timeout, for unchanged survivor eligibility checks to pass before
planning repair or advancing after detach. Metrics I/O and polling share that
absolute stage deadline; process-pin and transport errors remain terminal.
Persistent unsafe state still refuses membership changes, and its bounded
diagnostic includes blocked groups and maintenance reasons. This regression is
sequencing evidence, not reproduction of the unknown EKS rejection reason.
Final source `5a5088c5214193f121c205274ad2b68ba5e5c4bb` passed scheduled CI, real-S3 integration and both 128-owner WAL soaks. [Publication 37454063154](https://github.com/tonbo-io/ursula/actions/runs/37454063154) and [EKS 37454613056](https://github.com/tonbo-io/cloud/actions/runs/37454613056), Cloud `6f73567ed146fa1ef8dd61fc4d030aced0b9e10a`, passed. Both the 0.6.2 upgrade and complete sequential same-version restart verified all 1,024 external ACK payloads and offsets, hash `48ebe1593732a6e26b9e850193040c7ea63e2f4afe669fcb298817e0db81859d`. Source and artifact identities matched; namespace absence was independently confirmed. PR #378 merged as `eb3a52a7358f61eb3eb2bdee515a4732ae015276`, tree `7d31f8b16a9d856e53089d3a50d32a42b20d4f67`, exactly matching that qualified source. This is upgrade/restart regression evidence, not shared-reservation continuous-load, retained-volume or abrupt-host qualification. The #377 rollout consumer is rebased onto this repaired core and must receive new source-pinned checks, publication and production-maintenance qualification.

## M3 current review and rollout adapter work

Shared reservation policy merged in PR #376 as `14d89e378afe0e1cf4dd08eb9ae05b50e6f19223`. The merge tree `b27729f29d260ba258209d5b627e2c611f9dd493` exactly matches qualified head `dccabf0e1f0cbf1202760bca3b1e05b5c038b20c`, based on main `c066d14823fc90dfa6c50e0f3d7d9e4af5c33466`. Local final workspace tests/doc tests, formatting, all-target Clippy, eleven policy regressions, an actual CLI round trip, eight native cluster cases and seven DST audits passed; local S3 restart remains environment-gated. Remote CI 37439469140, real-S3 integration 37439469330, conformance 37439469462, ratchet 37439469370 and both 128-owner WAL soaks 37439469433 passed. No live Kubernetes CAS or complete maintenance consumer is claimed.

Rollout adapter work is isolated in `/private/tmp/ursula-maintenance-rollout`, branch `feat/ursula-maintenance-rollout`. The opt-in chart consumer now uses whole-object CAS acknowledgement before process activation, fresh all-active admission, normal UID-bound deletion, target-only binding, repair and all-retired completion. Offline helpers capture complete cell/source objects, build typed requests and project one validated snapshot. Explicit bootstrap is create-only and never part of ordinary hook execution. Once a store exists, disabling the value cannot reopen the legacy writer; an indeterminate GET also refuses legacy execution.

The shell/native CLI controller suite uses atomic synthetic transport and covers competing executors, missing/conflicting stores, failed proofs, serial source replacement, ambiguous deletion, bound-container restart refusal, partial retirement takeover, SIGTERM cancellation, no-op health and legacy bypass refusal. It is controller sequencing evidence, not live Kubernetes or Raft evidence. The native three-node memory restart separately passes actual all-active admission, a two-survivor pre-deletion prefix, target-only binding, six acknowledged payload readbacks and all-retired completion through the shared policy. Its physical metadata is a native-fixture identity, not a Kubernetes/provider fence.

That native check exposed a startup window: the old test helper waits only for metrics HTTP, so a first prefix could find an incomplete Raft recovery barrier. The consumer now waits for Pod Ready outside recovery and a fixed-plan complete-cluster eligibility check before beginning each fresh prefix observation. Fresh admission still certifies every group on all three original processes. Once a source UID is already gone, binding is observation only; the existing repair path validates both immutable survivors before membership mutations and allows only the selected target's remove/learner/promote interval. Completion still requires the full three-replica proof and cannot clear or lower the admitted prefix.

No serving deployment or live consumer qualification is claimed. Common managed-node admission, pre-fault physical inventory, automatic exact-host fencing/replacement, abrupt-host qualification and M4–M6 remain required; the Pod-only integration does not complete M3.


## Planned shared maintenance and legacy-node guard qualification

Ursula [#377](https://github.com/tonbo-io/ursula/pull/377) merged as `a08a5c76d3552235853f6ca4dfa3579febb45f43`, with the exact tree of qualified source `5b0f455509df73e6b10bcba497b2aa9d629e168f`. Production-config [EKS 37456170093](https://github.com/tonbo-io/cloud/actions/runs/37456170093), Cloud `6f73567ed146fa1ef8dd61fc4d030aced0b9e10a`, passed a complete serial three-voter planned Pod maintenance pass. All 34,756 acknowledged 4-KiB appends across 1,024 streams passed individual readback, full-prefix/exact-tail verification and duplicate replay. Temporary 82 HTTP 503s and five 502s were safely retried; no 416 occurred. Each completed reservation certified all 256 groups on all three replicas before selecting the next voter. Final proof completion at 151.081 seconds after the whole maintenance pass started is not a single-voter RTO or exact downtime. Namespace, hook RBAC and exact S3 versions/markers/multipart absence were independently verified after cleanup. Evidence is `/private/tmp/ursula-rollout-rebased-production-37456170093-evidence`. This qualifies synthetic planned Pod maintenance, not abrupt host recovery, retained production volume or resource stress.

Cloud [#3187](https://github.com/tonbo-io/cloud/pull/3187) merged as `be7e809a68656d2c2ef9a1bf918d0668f1512cf8`, tree `1ba73de5b795bf552e4b2cccc21f84c4788a4f18`, exactly matching source `cccd77ebf7b97f83dcc21108265459a206844e43`. Final-source Repository CI passed all 2,037 Python tests; Infrastructure CI and every scheduled merge-queue check passed. The old managed-node executor refuses any existing reservation store, including idle state, and failed observation fails closed before provider apply. Absence is not ownership. Common provider admission, terminal operation reconciliation and automatic fenced host replacement remain unfinished. No serving recovery was dispatched.


## Pre-fault host inventory policy

The current `ursula-ctl` change adds idle-only `publish_host_inventory` and a schema-2 catalog in the existing whole-ConfigMap CAS store. It retains all three Pod/Node/provider/failure-domain identities and pinned process plans, requires fresh full-group three-replica participation evidence, refuses changed physical hosts on healthy refresh, serializes with ownership, and preserves the catalog through takeover. Planned Pod completion advances the selected Pod/process identity and full-prefix observation atomically with release; a catalogued planned Pod transition cannot move to another physical host. No new coordination store or disruption authority is introduced. First migration requires settled legacy executors/provider operations and cannot reconstruct pre-fault identity after the event.

Five additional policy tests cover concurrent capture/ownership receipts, sixteen incomplete/uncertified capture cases, changed/reused hosts, healthy process recovery, prefix nonregression and planned completion. Actual CLI capture/proposal/acknowledgement/readback and the existing controller sequencing suite pass. The native three-node memory-voter restart consumes live six-group pre-fault and completion observations, retains six acknowledged payloads, and advances the catalog after repair; physical metadata remains synthetic. Final local workspace unit/bin tests passed 901 tests with one pre-existing ignored stress test; doc tests, all-target Clippy, formatting and seven DST audits passed. All eight native cluster entries also passed; real-S3 restart is environment-gated locally. Remote checks remain a separate gate. Automatic sampling, two-survivor host admission, irreversible provider termination, persistent stale-Pod retirement intents, common managed-node admission and abrupt-host qualification remain outstanding M3 work.
