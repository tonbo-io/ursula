# Shared maintenance reservation policy

The `ursula-ctl::reservation` policy and the offline `reservation-propose` /
`reservation-acknowledge` commands provide a common, fail-closed state machine
for **planned, UID-bound Pod replacement and exact-host recovery intents**. They perform no Kubernetes writes,
provider calls or Raft mutations. The opt-in chart consumer described below uses
this policy for planned Pod rollouts. Managed-node writers and abrupt host recovery
are not yet integrated, and serving cells are not yet qualified.

## Store and ownership

The persistent ConfigMap is `<StatefulSet>-maintenance`. It must have no hook
annotation, owner reference or deletion timestamp. Its `data.reservation` is one
JSON document; reads must capture the complete ConfigMap, not independent fields.
The expected cell includes namespace and StatefulSet UIDs, three voter IDs
`1,2,3`, group count and core count. Missing or recreated stores never become a
new reservation automatically. Initial generation-zero state is an explicit,
reviewed bootstrap, independent of ephemeral executor Jobs.

A reserve request selects exactly one source Pod UID, Node UID, provider identity
and process incarnation, plus a fixed three-process plan. It creates one canonical
operation/executor token at the next checked generation. Another reserve is
refused while this operation exists. There is no timeout-based release. Takeover
keeps the same operation, source, replacement and admitted prefix boundary; it
changes only the executor and increases the global generation. It cannot refresh
survivor process pins or select another source.

The adapter preserves the whole object and its UID/resourceVersion. The platform
must submit `kubectl replace`, stop on conflict, and retain the successful API
response. Acknowledgement requires that exact proposal's data, the same immutable
store UID and a different resourceVersion. A proposal, dry-run, losing update or
recreated ConfigMap is not a receipt. This follows Kubernetes' [update conflict
contract](https://kubernetes.io/docs/reference/using-api/api-concepts/#updates-to-existing-resources).
No numeric ordering of resourceVersions is assumed.

## Planned Pod replacement transitions

1. Acquire the shared reservation by CAS. Activate its token on all pinned
   processes using the existing executor-admission commands. An offline receipt
   alone does not activate servers or authorize a deletion.
2. Obtain a fresh schema-3 `verify-quorum` observation, started after acquisition
   and no more than 60 seconds old at the supplied observation clock. Every group
   must have a fixed prefix applied by all three original process incarnations,
   and every process must hold the same active executor token. CAS
   `admit_pod_deletion` before a physical delete. Its prefixes cannot regress below
   the previous completed operation or this operation's prior admission.
3. Use the existing Kubernetes `DeleteOptions.preconditions.uid` transport to
   delete only the admitted source UID. Never replace that UID by a name lookup.
   Admission remains sticky after failure, cancellation or takeover.
4. After the original UID is gone, capture the same-name replacement Pod and its
   scheduled Node as complete API objects. The Pod must belong to the pinned
   StatefulSet, have a different UID and process incarnation, and neither object
   may be deleting. `bind_pod_replacement` accepts only a target-process change;
   addresses, token, inventory and both survivor process pins remain fixed. A
   second replacement binding is refused. The Node's provider identity is stored
   as an opaque value; this observation **does not fence the original host**.
5. Complete repair and all mutating admin work, retire the current token on all
   three processes, and obtain a new schema-3 all-retired prefix observation from
   the bound process plan. Mixed active/retired tokens, missing groups/replicas,
   stale processes or a lower prefix cannot release the operation. CAS
   `complete_pod_replacement` stores the completion receipt and retains the global
   high-water generation. A next reserve must still match the completed process
   incarnations. There is no unconditional operation-clear transition.

Even a healthy container restart within the **original Pod UID** cannot satisfy
step 4. Otherwise a delayed old UID-bound deletion could still destroy that
voter after the reservation was released and another voter was disrupted.
Takeover also retains the original admission boundary: recertification must not
turn missing data into a new, lower accepted prefix.

## Pre-fault physical inventory

The optional `publish_host_inventory` transition records every voter's source identity, Node name and `topology.kubernetes.io/zone` in the same persistent ConfigMap. This is an idle-only whole-object CAS migration from schema 1 to schema 2: it preserves the store UID, global generation and any completion receipt, and races with ownership through the same resourceVersion. A missing store cannot be initialized by capture. Schema-1 consumers reject schema-2 state; deploy a supporting CLI before this explicit migration. The current chart does not publish inventories automatically.

Before the first inventory migration, stop every legacy executor and reconcile any pending provider operation and previous physical owner. Capture does not reconstruct the original host of an already-observed fault or prove a former owner has stopped. Enabling automatic recovery requires a healthy, settled pre-fault inventory; neither a recreated Node nor a new store may establish that history retroactively.

Capture requires three nondeleting Ready Pods and Nodes, exact StatefulSet ownership, distinct Pod/Node/provider identities and failure domains, a complete pinned process plan, and a fresh schema-3 full-group/all-replica participation proof. Kubernetes Ready alone is insufficient. The observation may have no executor, or certify the last completed executor as retired; active or unrelated executor evidence is refused. The platform must sample complete identities before and after the Raft observation, reject changed objects/processes, and submit the exact resulting proposal. The policy validates supplied observations; it cannot authenticate them or make separately sampled Kubernetes and Raft objects atomic.

Later healthy capture may update Pod/process incarnations only on the same Node UID, provider identity, Node name and failure domain, after full nonregressing Raft recovery. A recreated same-name Node cannot replace the old host record. A physical host change requires the fenced host transitions below. Once an operation is reserved, capture is refused; takeover retains the entire catalog. A catalogued planned Pod replacement must stay on that physical host. Completion updates its selected Pod/process identity and the all-retired write boundary atomically with release. Existing schema-1 planned consumers keep their current behavior until migration.

Build the capture request from Kubernetes List objects and a fresh `verify-quorum` observation, then use the existing proposal/acknowledgement interface:

```sh
ursulactl reservation-request publish-host-inventory \
  --pods pods.json --nodes nodes.json --config pinned-processes.json \
  --observation quorum.json > request.json
ursulactl reservation-read --cell cell.json --snapshot committed.json --field hosts
```

Capturing inventory grants no disruption authority and proves no physical host fence. Automatic capture/reconciliation, an authenticated provider adapter, safe startup ownership, common managed-node admission and abrupt-host qualification remain required before automatic host recovery can be claimed.

Verification files and API receipts are operational evidence, not authenticated
capabilities. The reviewed platform adapter must actually obtain them, supply its
current clock, reconcile the selected physical identities and activate/retire
server tokens. This offline policy cannot make a stale local receipt current or
prove that a provider operation completed.

## Exact-host recovery transitions

The host path upgrades the same persistent document to schema 3; it never creates a parallel lock. Schema-2 consumers must refuse this version. The planned chart rollout reads `operation-kind` before takeover and refuses `host-recovery`. Migration must reconcile former executors and asynchronous provider operations; schema rejection does not revoke actions they already submitted.

1. `reserve_host_recovery` selects one physical owner from the settled pre-fault inventory. Source identity and all addresses remain fixed. Healthy survivor boots may be pinned anew at initial acquisition, but both must then supply fresh quorum proof and remain immutable through takeover. Another disruption cannot acquire this reservation.
2. Activate the current token on both pinned survivors. `admit_host_termination` requires a fresh `verify-surviving-quorum` observation explicitly excluding the source, covering every configured group on both certified survivors and preserving the catalog/completion/admission prefix floor. It persists the intent to terminate only the original provider instance before any provider call.
3. The provider adapter must authenticate its response and observe that exact instance irreversibly `terminated`. `record_host_termination` rejects `stopped`, `shutting-down`, unknown/absent instances, another provider identity, stale observations and observations preceding admission. The recorded receipt is sticky across takeover and provider record expiration. A termination request or local JSON file alone is no physical fence.
4. Before any forced Pod deletion, CAS `admit_fenced_pod_retirement` retains the exact UID as a permanent tombstone for this operation. An observed Pod, including the original UID, must be on the original Node UID/provider instance; a recreated same-name Node is refused. With no Pod/Node objects, the transition records only the original catalog UID tombstone. This does not authenticate a presently running Pod or authorize deletion on a different physical host. The history is bounded to 32 UIDs and retained in completion. A candidate UID with a retirement intent can never become the bound replacement, so a stale executor's delayed delete cannot target an accepted replacement. Binding and additional retirement intents race through the same CAS; no new retirement is admitted after binding.
5. `bind_host_replacement` requires the terminal receipt and original UID tombstone, a nondeleting owned replacement Pod, a Ready replacement Node, new Pod/Node/provider/process identities, the original failure domain and a physical host distinct from both survivors. Only the selected target boot may change. A second binding or a changed survivor refuses progress.
6. Repair membership and catch-up, complete all mutations, retire the current token on all three current processes, and collect fresh all-group/all-replica proof begun after terminal fencing. `complete_host_replacement` requires the admitted prefix floor and unchanged survivor boots; it atomically updates the selected physical catalog entry, retains termination/tombstones in completion and releases ownership. Partial proofs or failed recovery leave the reservation held.

Takeover preserves the exact source instance, survivor boots, termination intent/receipt, every deletion tombstone and any bound replacement. It advances only executor/generation, with no expiry-based release. API retries must use those persisted identities; name lookup cannot replace them.

The platform consumer must reconcile current Pod/Node/provider identities around every action, delete with exact Kubernetes UID preconditions, and prevent retired/unbound Pods from starting a new physical owner. In particular, Kubernetes `spec.nodeName` is name-based: a new Node with the old name can inherit an existing Pod UID. Terminating the old instance does not fence that new instance, and the original UID tombstone is not permission to force-delete its process. Unknown physical ownership must stop recovery. These consumer/startup checks, actual provider calls, managed-node operation reconciliation and live fault qualification are still outstanding; this offline state machine does not implement automatic host recovery by itself.

## Offline interface

`--cell` is the expected `CellIdentity` JSON; `--snapshot` is one full ConfigMap
GET response; `--request` is the explicit JSON transition. Each input is bounded
at 2 MiB. The request's `action` is `reserve`, `takeover`, `admit_pod_deletion`,
`bind_pod_replacement`, `complete_pod_replacement`, `publish_host_inventory`, `reserve_host_recovery`, `admit_host_termination`, `record_host_termination`, `admit_fenced_pod_retirement`, `bind_host_replacement` or `complete_host_replacement`. Reserve/takeover and host capture supply
`now_ms`; every progress request carries its exact current `fence`. Prefix
observations have the unchanged CLI output shape
`{ "started_ms": ..., "completed_ms": ..., "verification": ... }`.

```sh
ursulactl reservation-propose \
  --cell cell.json --snapshot before.json --request request.json > proposal.json
# The reviewed platform adapter submits this exact object, retaining its result.
kubectl replace -f proposal.json -o json > response.json
ursulactl reservation-acknowledge \
  --cell cell.json --snapshot before.json --request request.json \
  --response response.json > acknowledged.json
```

The acknowledgement output includes the complete reservation and its fixed node
plan when active. `disruption_authorized` and `physical_hosts_fenced` remain false:
the command validates a persisted transition, not the subsequent physical action.

## Chart consumer and explicit bootstrap

Set `server.updateStrategy=OnDelete`, `server.gracefulRollout.enabled=true` and
`server.gracefulRollout.maintenanceReservation=true` after all three voters
support executor admission. This selects `files/maintenance-rollout.sh` instead
of the legacy adapter. `expectedGroups` must match `raft.groupCount`, and
`server.coreCount` supplies the same core inventory as the generated server
configuration. The Job has read-only Node/namespace access for immutable identity
capture (`Node.spec.providerID` must be present), and get/update access to its
exact reservation ConfigMap; it cannot
create/reset that store or mutate Node objects.

Create the persistent store once as a separate, reviewed new-cell bootstrap:

```sh
kubectl get namespace "$namespace" -o json > namespace.json
kubectl -n "$namespace" get statefulset "$statefulset" -o json > statefulset.json
ursulactl reservation-cell --namespace-object namespace.json \
  --statefulset-object statefulset.json --group-count 256 --core-count 2 > cell.json
ursulactl reservation-bootstrap --cell cell.json --confirm-new-cell > initial.json
kubectl create -f initial.json
```

Use the actual cell group/core counts. The generated document has no UID,
resourceVersion, owner or hook; create obtains the platform identities. An
existing store makes create fail. Never run this procedure to recover a deleted
reservation for an existing cell: it discards the persisted generation and any
unfinished operation. Ordinary hook execution has no bootstrap branch.

The consumer submits whole-object replace and validates the returned API receipt
before activating process tokens. It uses all-active fresh prefix evidence after waiting for fixed-plan Raft eligibility before
recording deletion admission, a UID-bound normal Pod delete, target-only process
binding, catch-up, and all-active then all-retired fresh evidence before completion.
An already-staged image alone cannot pass a no-op rollout without complete group
health. No legacy identity/eligibility allowance is used in the reserved path.

A failed or cancelled hook stops its tunnels and leaves the reservation intact.
A new executor takes over the same operation at a higher generation. After an
accepted but ambiguously observed delete, it resumes that original target and
retains both survivor boots and the admission prefix floor. Binding an already
retired source is observation only: repair validates the fixed surviving pair
before membership changes, without demanding a complete three-voter membership
during the selected target's remove/learner/promote interval. Once the replacement
is bound, another container restart or Pod UID change refuses advancement. Partial
token retirement is reconciled by a higher-generation takeover before release.
Another source cannot be reserved before completion preserves a full-group receipt.

## Outstanding host work

`maintenanceReservation=false` retains the existing unconditional-apply adapter
only for explicitly uncertified legacy migration. Legacy voters without executor
admission need this controlled migration before strong takeover is enabled. Once
the persistent store exists, the legacy Job refuses to run even if a later values
change disables the reserved path; a failed store GET also fails closed. Bootstrap
requires that every legacy executor has already stopped.
Both planned rollouts and managed-node writers must eventually use the common
reservation before they can share one exclusion boundary. External uncoordinated
force deletion or Node/provider mutation is not qualified by this Pod-only path.

Abrupt host recovery requires irreversible termination of the exact old provider
instance before force-removing Kubernetes identities. EKS managed-node updates
also require persistent operation/idempotency and terminal reconciliation; a timed
out or unknown provider request cannot be released into another disruption. Actual provider execution and managed-node operations remain **unimplemented platform consumers**; typed host intents do not authorize them. Current
physical identities must be captured before a host fault, rather than inferred
from a recreated Node. Isolated EKS fault qualification and serving rollout remain
separate gates.

## Validation boundary

Native policy tests cover concurrent CAS contenders over a mock HTTP API,
recreated/ephemeral stores, fixed-source takeover, stale/future and incomplete
prefixes, same-UID refusal, target-only rebinding, retired-token completion, prefix
nonregression and retained generation. An actual `ursulactl` subprocess verifies
proposal/receipt separation and refusal to adopt another committed state. These
are not live Kubernetes CAS, host-fencing or production RTO evidence. The
chart shell/native CLI sequencing suite uses atomic synthetic platform transport
and covers concurrent actual consumers, missing/conflicting stores, incomplete
proofs, serial source replacement, ambiguous deletion, fixed replacement boots,
partial retirement takeover, SIGTERM cancellation, host-ownership takeover refusal and no-op health checks. Raft proofs in that suite
are synthetic. A separate native three-node restart feeds live all-active and
all-retired prefix observations through the policy and verifies six acknowledged
payloads; its physical metadata is a native fixture, not a provider receipt.
Real Kubernetes consumer and production-scale fault qualification remain required.

The additional native abrupt-loss test SIGKILLs one memory-WAL process without preparation, verifies two-survivor live prefix admission and continued idempotent writes, takes over the same host intent, repairs a new target boot, replays each acknowledged producer sequence at the original offset and checks exact payloads on all three replicas across six groups before all-retired completion. Physical identities and the terminal-provider observation remain fixtures derived from the exited child, not AWS/Kubernetes qualification. The actual offline CLI test exercises all six host request builders, integer-map-key proof deserialization, whole-object proposals/receipts and completed physical-catalog advancement.
