# Shared maintenance reservation policy

The `ursula-ctl::reservation` policy and the offline `reservation-propose` /
`reservation-acknowledge` commands provide a common, fail-closed state machine
for **planned, UID-bound Pod replacement**. They perform no Kubernetes writes,
provider calls or Raft mutations. They are not yet wired into the chart or Cloud
workflow and do not make those existing consumers certified.

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

Verification files and API receipts are operational evidence, not authenticated
capabilities. The reviewed platform adapter must actually obtain them, supply its
current clock, reconcile the selected physical identities and activate/retire
server tokens. This offline policy cannot make a stale local receipt current or
prove that a provider operation completed.

## Offline interface

`--cell` is the expected `CellIdentity` JSON; `--snapshot` is one full ConfigMap
GET response; `--request` is the explicit JSON transition. Each input is bounded
at 2 MiB. The request's `action` is `reserve`, `takeover`, `admit_pod_deletion`,
`bind_pod_replacement` or `complete_pod_replacement`. Reserve/takeover supply
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

## Outstanding consumer and host work

The chart's existing rollout state still uses unconditional apply and is explicitly
uncertified. Both it and Cloud must use the common reservation before their
writers can share one exclusion boundary. Legacy voters without executor admission
need a separate, explicitly uncertified migration before strong takeover is used.

Abrupt host recovery requires irreversible termination of the exact old provider
instance before force-removing Kubernetes identities. EKS managed-node updates
also require persistent operation/idempotency and terminal reconciliation; a timed
out or unknown provider request cannot be released into another disruption. Those
operations are deliberately **not admitted by this Pod-only policy**. Current
physical identities must be captured before a host fault, rather than inferred
from a recreated Node. Isolated EKS fault qualification and serving rollout remain
separate gates.

## Validation boundary

Native policy tests cover concurrent CAS contenders over a mock HTTP API,
recreated/ephemeral stores, fixed-source takeover, stale/future and incomplete
prefixes, same-UID refusal, target-only rebinding, retired-token completion, prefix
nonregression and retained generation. An actual `ursulactl` subprocess verifies
proposal/receipt separation and refusal to adopt another committed state. These
are not live Kubernetes CAS, chart integration, host-fencing or production RTO
evidence.
