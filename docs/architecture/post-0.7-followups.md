# Recovery follow-ups and maintenance convergence

Status: proposed split and design for review. This document changes no runtime
behavior. The baseline is main `c374f709` (#427 and #428), compared with frozen
#426 at `82a3f635`. The frozen branch is evidence and a source of candidate
patches, not an implementation to merge wholesale.

## Remaining scope

#405–#409 are merged. Preserve #428's durable genesis floor and strengthened
fault preconditions when extracting anything from #426. In particular, a
persisted genesis vote is different from a missing vote.

| Issue | Current boundary | Next independently reviewable change |
| --- | --- | --- |
| #410 | The legacy incarnation CLI flag still exists on main. | Delete the option and permissive branch, retain fail-closed identity parsing and CLI tests. |
| #411 | Recovery risks remain individually open. #427/#428 do not prove every case. | Separate genesis-without-membership, missing run state, failed poison-marker persistence, delayed pre-reboot ACK, and snapshot/directory durability cases. Each needs a reproduced regression or an explicit recovery decision. |
| #412 | The Raft library still mixes native filesystem tests and simulation tests. | Classify tests, run eligible tests with the simulation runtime, explicitly gate native-only tests, and add the actual Raft suite to madsim CI. Do not hide failures by gating safety tests without equivalent coverage. |
| #413 | Registry construction, metric contracts, WAL exports and typed errors are distinct tasks. | Start with shared fail-closed admin metrics, then single-path registry construction. Keep WAL ownership/export cleanup and error conversion as separate changes. |
| #414 | Thread-per-core work includes independent hot-path and lifecycle changes. | Measure each proposed change first. Separate read forwarding, admission/counters, HTTP ownership and background task lifetime. |
| #415 | The frozen serving/maintenance split is a candidate, not a verified deployment result. | Separate traffic readiness from permission to disrupt another replica. Keep main's maintenance mechanism until its replacement is supported. |
| #416 | A pure control-state foundation exists on main. The frozen executor and identity design need review. | Follow the design and dependency order below. |
| #417 | The frozen allocator cleanup is independent of control-plane changes. | Use dhat for runtime/stream allocation assertions in isolated test binaries. Keep the state probe's bucket-counting allocator. |
| #418 | The frozen patch isolates some apply failures and supports replay with corrected code. | First ship isolation and diagnostics. Decide supported recovery semantics separately before closing the issue. |
| #419, #420 | Performance comparison and extended Chaos/soak qualification. | Require separate acceptance evidence; code-level tests here do not satisfy these issues. |

## Extraction rules

Extract against current main, not by rebasing all of #426. Keep every change
small enough to explain with one failure or one boundary. Each PR states its
remaining limitations and closes only the issue requirements it actually meets.

Preserve meaningful safety assertions. A test must establish its fault
preconditions, including an acknowledged write, the intended leader/vote,
actual page loss or a real pending action. Prefer barriers and notifications
to increased timeouts. Baseline failures and fixed results use pinned commits.

Do not bring over `process_fence_measurement.rs` wholesale. The frozen module is
already gated by `cfg(all(test, not(madsim)))`, so its location under `src` does
not make it production code. Move useful admission regressions beside the
relevant implementation, discard obsolete per-RPC meta-read measurements, and
put a retained performance measurement in a benchmark with an explicit question.

Do not carry dead migration commands alongside the replacement operation model.
Do not delete the ConfigMap reservation, startup admission or rollout tooling
until the replacement supports their required safety behavior. Their removal
belongs to the final cutover PR, with a working ordinary-PVC-restart runbook.

### Specific #411 follow-ups

- Membership records must survive an immediate full-cluster power loss under
  `never`. Evaluate the frozen membership-fsync change independently, with a
  real multi-voter regression and its fsync cost. Preventing new occurrences
  does not recover existing disks that all lack membership.
- A missing run-state must not reset the recovery epoch below a surviving
  core's verified epoch. Cover multiple cores, repeated marker loss and
  overflow without restoring the old permissive sealed-segment replay.
- Inject failure while recording `poisoned`, including write, rename and sync,
  after partial journal writes and fsync EIO. Test both same-boot and host-crash
  recovery. A surviving page-cache prefix alone is insufficient coverage.
- Delay a real replication response across reboot and recovery. Assert it
  cannot make the leader commit using a retired replica's acknowledgement.
- Route snapshot metadata through the same journal I/O abstraction and audit
  publication ordering: temporary-file write, file sync, rename, parent-directory
  sync, pointer publication, then purge. The current direct `std::fs` path is
  invisible to SimDisk power-loss schedules. Test each boundary under both WAL
  fsync policies, including restart after a lost directory entry; an in-memory
  pointer or surviving page-cache prefix is not durability evidence.

The snapshot parent-directory open error is a separate immediate fix, not gated
on the wider #411 audit or the control-plane design. Main's
`persist_snapshot_metadata` silently skips directory sync when `File::open(parent)`
fails. Propagate the open error, preserve the previous published pointer, and
cover the failure before the 0.7 release. This narrow fix does not by itself
bring snapshot metadata into the simulated I/O path.

## #416: authority and availability

Meta owns desired placement, node lifecycle and maintenance intent. Each data
group owns its effective Raft membership and installed replica identities.
These are different facts. A meta projection cannot override the data group's
committed membership or declare a replacement ready before the data group has
accepted it.

A managed group has one control authority. Loss of meta quorum does not switch
it to static configuration or an in-memory authority. Configuration supplies
bootstrap addresses, not a replacement placement map on every restart.

The proposed rollout preserves current static deployments until an explicit
managed-mode transition is supported. A standalone durable node must keep
working with only `wal.path`, without configuring remote meta peers. Managed
mode is explicit and durably recorded. Static mode uses the existing data memberships and exposes no new dynamic
maintenance API. Managed mode uses meta exclusively for control intents. These
are mutually exclusive persisted deployment modes, not an `enabled=false`
fallback inside a managed group. Static and managed writers must never
operate concurrently on the same group. The conversion protocol, storage epoch
and any unsupported upgrade boundary must be reviewed before enabling it.

The following table states the proposed acceptance contract, not the frozen
implementation's current behavior.

| Condition | Required behavior |
| --- | --- |
| Meta leader changes or meta loses quorum | Existing admitted data groups continue replication, elections and client traffic using durable group-local authority. Membership-changing operations wait. |
| Data RPC or post-commit response | No meta ReadIndex call. A committed write is not changed into a failure because meta is unavailable. |
| Same-volume restart | Recover durable local identity, membership and fences. Existing data groups do not require a new meta quorum just to resume. Maintenance actions wait for an authorized executor claim. |
| Lost-volume replacement | Fresh identity remains unadmitted until the replacement protocol establishes current membership, prefix and fence evidence. |
| Node never admitted to the cluster | It cannot bootstrap an existing group from addresses or a missing local file. |

The frozen implementation removes meta reads from normal data RPCs, but its
startup still performs a linearizable meta read and `RestartProcess` before
starting data actors. The same-volume restart behavior above is therefore a
new acceptance requirement, not a claim about #426.

A live topology view must drive routing, engine ownership, readiness and admin
observations consistently. Mark observations stale or unavailable when their
source cannot be refreshed. Missing safety fields must not deserialize to
"ready". A local serving decision and authorization for another disruption
remain separate answers.

## Durable meta storage

The meta Raft log is an append log through the same WAL I/O abstraction as the
data groups, in native and SimDisk builds. Do not port frozen #426's full-file
rewrite on every append, and do not add a separate direct-`std::fs` storage path.

The design choice is to store meta as an internal logical group in the existing
per-core journal, reusing its writer, framing, checksums, segment lifecycle,
replay and durability barriers. It is not a drop-in data-group allocation:
`CoreJournalRecord` currently carries `UrsulaRaftTypeConfig` entries and a data
`group_id`. The storage change must introduce a typed record/group namespace for
meta, keep its identity outside client-visible shard routing, and define the
storage-format upgrade boundary. A second WAL implementation needs a concrete
reason that this shared representation cannot support the required lifecycle;
a different Raft command type alone is not that reason.

Test recovery under both `always` and `never`, including a full-cluster power
loss immediately after an acknowledged meta append. A data WAL policy of
`never` does not permit acknowledged control ownership, process epochs or
activation records to roll back. Specify and enforce the meta record's durable
acknowledgement boundary through the shared writer, including directory entry
durability, and measure any extra fsync cost. Snapshot install and compaction
must retain the same committed prefix and authority after restart. Vote,
truncate, purge and snapshot-pointer recovery belong in the same native/SimDisk
fault matrix, rather than only testing a successful append/reopen.

## Replica identity fence

Separate the lifetime of a replica's storage from a process boot and from an
operation executor claim. A normal restart retains the replica identity.
A new disk gets a new identity. Executor generation fences duplicated or
reassigned maintenance actions, without changing the data identity on every
boot.

Use one group-local admission policy in the Raft layer. gRPC and in-process
transports call it, and the HTTP layer only translates typed outcomes. One
shared topology predicate answers whether a node hosts, or is being prepared
to host, a group. The registry routes to these components instead of owning
another copy of their policy. Hosting eligibility does not authorize opening
an actor: `Prepare` must supply a separate admission certificate before a
pending replica can campaign. Consolidating the hosting predicate must preserve
that distinction.

Both sides of every replication exchange must be bound to their identities:

1. The receiver checks the sender and the intended receiver identity before
   processing a heartbeat, vote, append or snapshot.
2. The caller binds the outstanding request to the expected responder identity
   and group generation. It validates that binding before a response can count
   toward a quorum or replication progress.
3. Append streams validate every frame and response. Reused connections and
   addresses do not certify identity. A delayed reply from a retired process
   cannot satisfy a request for its replacement.

The frozen receiver checks its own local identity and the sender's installed
identity. However, the request does not bind the caller's expected receiver
identity, and the acknowledgement does not certify that responder. Rechecking
the local sender after a response does not establish the responder's identity.
The implementation PR must trace this contract through OpenRaft's actual
response bookkeeping and reproduce a zombie responder plus subsequent leader
loss. A hand-issued heartbeat whose future returns an error is insufficient.

### Replacement activation

The proposed conservative protocol is:

1. Meta records the replacement intent and its pinned participants. It does
   not yet activate a replacement voter.
2. The current data group replicates an identity-fence entry with its required
   prefix. This entry does not itself change voter membership. Any removal,
   demotion or promotion uses a separately ordered Raft membership transition.
3. Every retained voter required by the transition durably applies the fence
   and acknowledges the exact transition. Required acknowledgements come from
   a recorded membership set, not a best-effort list of reachable processes.
4. The replacement catches up as a learner, proves its identity and prefix,
   and is promoted through Raft. Meta then commits the resulting placement.

An unreachable required participant blocks activation unless an explicitly
committed membership change safely excludes it. A timeout is not retirement
proof. The identity PR must name the required sets for every stable and joint
membership phase and prove their quorum intersections, including late replies,
before this protocol is enabled.
The vote-floor protection from #427/#428 remains required after lost history.

Persist fence state through the same I/O abstraction used by WAL and snapshot
recovery, including file and directory durability. No direct `std::fs` side path
that disappears from simulated power loss. Fence durability must hold under
both data WAL fsync policies. Define the atomic recovery boundary between the
installed fence, applied index and snapshot before choosing its encoding.

## Operation kernel and executor

Keep `ursula-control` pure: typed commands, enum phases, deterministic
transitions and property tests, with time supplied as input. Use one operation
model for move, rebuild and decommission. Remove the old migration model in
the same change that replaces its callers and persisted representation.

Keep the async executor in a dedicated module outside the HTTP routing layer.
It interprets the kernel's actions, uses narrow Raft/storage interfaces and owns
its tasks. HTTP and `ursulactl` parse, submit and render the shared admin types.
They do not schedule membership transitions or make independent fence decisions.
Replace string constructors at these boundaries with structured errors that
preserve their source and retry classification. Moving `MetaRaftError::new`
into a different file does not satisfy this requirement. Registry method count
and duplicated policy state should decrease as these responsibilities move.

Each action has a durable identity and typed outcome: not dispatched, completed,
or outcome unknown. After cancellation, leader change or restart, reconcile an
unknown outcome before retrying it. Reassignment needs proof that the old action
cannot later mutate membership. A timer or a newer executor claim alone is not
that proof. Interrupted operations may remain safely blocked when evidence is
unavailable, and must expose the exact blocking condition.

Restart recovery applies to every participant, not just the node currently
executing an action. Same-volume participants reconstruct state from durable
receipts and membership without requiring a rebuild. Deferring a boot claim while meta is unavailable
does not authorize rebinding an old executor receipt to the new process.
Property tests cover
transition invariants, while production-path tests cover I/O and cancellation.

## Node lifecycle

Joining first registers a non-voting meta learner and authenticates its identity.
It receives no data voter role from registration alone. Assign data as learners,
verify catch-up, and promote through each group's membership protocol.

Decommission prepares and catches up replacement replicas before removing the
source data voters. Only after data placement is safe may it remove the source's
meta role and complete retirement. Cover nodes with no data groups, removal of
the current meta leader, and a durable retirement tombstone that refuses an old
process returning at a reused address. Aborts before irreversible membership work
can discard the intent. Later interruption reconciles forward or performs an
explicit safe reverse transition, rather than pretending nothing happened.

Authenticate meta peer traffic and control writes at the transport boundary.
Define credential bootstrap, rotation and failure behavior in the meta transport
PR. Do not introduce per-data-RPC meta reads to implement authorization.

## PR order and acceptance evidence

Independent work can proceed alongside design review:

- #410 and #417 each get their own cleanup PR.
- #412 classifies native/simulated tests and exposes the intended CI suite.
- #413 shared metrics and #415 readiness can follow together as dependent small
  PRs. Test healthy survivors serving during a real three-node rebuild while
  maintenance safety refuses a second disruption. Verify the rebuilding pod
  stays unready and the deployed Service/PDB behavior.
- #418 isolation requires panic, infrastructure error and normal business-reject
  cases. A partially mutated failed group cannot serve or publish snapshots.
  Another group on the same owner must keep working. Preserve the failed WAL
  and prove corrected-code replay. A multi-replica poison-pill drill and the
  supported operator recovery contract remain separate acceptance requirements.
- #414 changes require a baseline profile and bounded-progress tests. Existing
  biased selection in the Raft owner does not prove the new runtime service
  queue is fair under continuous load.

After this design is reviewed, #416 proceeds in dependency order:

1. One pure operation model, including recovery outcomes and transition tests.
2. Append-only meta storage in the shared per-core WAL and authenticated
   transport with explicit managed-mode startup. Validate both fsync policies
   through the native/SimDisk I/O seam. Keep it unenabled until the remaining
   integration is complete.
3. Encapsulated request/response identity admission and simulated durable fences.
4. Thin executor with participant restart and cancellation reconciliation.
5. Complete live topology, join and decommission, followed by removal of the
   superseded CLI/Kubernetes mechanism and its obsolete documentation.

For steps 3–5, test every operation phase against source, target, unaffected
voter, data leader, meta leader and executor restarts. Include same-PVC restart,
whole-volume loss, meta outage, lost responses, duplicated actions, executor
handover and a stale process at a reused address. Assert committed data remains
readable and operations complete, safely abort, or report a durable explicit
block while required evidence remains unavailable. Once connectivity and the
required storage assumptions are restored, the operation must converge to
completion or a supported abort. A crash loop is never a valid recovery state.

Design review must settle managed-mode conversion, fence quorum intersections
and durable encoding, unreachable-participant handling, and supported apply
recovery semantics. Until those decisions are reviewed, the frozen implementation
is not a supported operational path and this document closes no issues.
