# Meta Raft membership and maintenance

Meta Raft stores desired data-group placement, durable replica identities, boot
incarnations and epochs, and the single active maintenance operation. Its journal and snapshots are under
`<raft.wal.path>/meta-raft`, separate from the per-core data journals. Metadata is
checksummed, exclusively locked, and fsynced independently of the data WAL's
`fsync` policy.

A process claims a boot epoch for maintenance actions before serving traffic.
Data RPCs instead carry the durable WAL-lifetime replica identity, including each
frame of an append stream. Group-local durable identity fences reject retired
replicas without a metadata quorum read on every data RPC.
The live placement view drives routing and engine ownership; removing a replica
from committed placement prevents the startup configuration from reopening it.

## Operations

The admin `POST /__ursula/control/operation` endpoint accepts typed intent.
`ursulactl operation --config nodes.json --node-id NODE --request intent.json`
is the platform adapter. It neither owns a Kubernetes ConfigMap lock nor accepts
caller-provided prefix evidence. The server obtains a leader ReadIndex proof and
checks the pinned replicas against that fixed prefix through their configured
private cluster endpoints.

- `MoveReplicas` first commits each new target identity into the existing group's
  Raft log and certifies durable quorum application. It then prepares target
  engines, adds learners, changes voters and commits placement after evidence
  confirms all desired voters have caught up.
- `RebuildReplica` first proves the surviving data quorum, removes the source
  from data and meta voters while retaining it as a learner, and retires its process
  epoch. The replacement claims a new epoch after metadata catch-up. Reconciliation
  restores its previous meta membership role; completion requires data-group recovery too.
- `DecommissionNode` first prepares replacements, catches them up and commits the
  desired data voter sets. Only then may `RetireSource` remove the source from meta
  membership. Completion records the node as removed after fresh data evidence.
  A node whose last data replicas were already moved can still be decommissioned.

A normal restart using its existing persistent volume claims a new process epoch;
it does not require a replica-rebuild operation.

For Kubernetes, keep the StatefulSet on `OnDelete`. For an ordinary upgrade,
drain leadership through `ursulactl`, stop one pod gracefully, preserve its PVC,
and wait for maintenance readiness before restarting another voter. There is no
automatic pod-deletion hook or Kubernetes reservation store. For a lost volume,
submit `Begin(RebuildReplica)`, `CollectEvidence`, then `RetireSource`; replace
only the explicitly retired node's volume, start its replacement, and submit
`Reconcile`, `CollectEvidence`, then `Complete`. Preserve the returned operation
token in each intent file. The server derives the process identities and evidence.

Operations pin participant identities and an executor generation. Takeover keeps
prefix floors and unresolved action receipts. A membership action is bound to one
leader process and serialized with duplicate requests. Completion cannot discard
an unresolved receipt. Normal leadership changes drain and fence the exact old
receipt, confirm the new leader through a fresh quorum proof, then reassign the
same immutable action under a new sequence. They do not retire a healthy process.

`RecoverAction` requires a serialized drain acknowledgement from the bound action
executor and fresh survivor evidence before retiring its epoch and reassigning the
receipt. Draining fences delayed duplicate requests as well as waiting for an
already admitted mutation. An unreachable executor without this proof leaves the
operation pending; timeout or a newer watch value is not proof that an old queued
membership mutation cannot execute.

## Joining a new node

Submit `JoinNode` to an existing node's admin operation endpoint with a new
`node_id`, `client_url`, `cluster_url` and `meta_url`. Registration adds a non-voting
meta learner; it does not increase the voting quorum or assign any data group.
Configure the new process with fresh storage, the same meta credential and the
existing meta peers plus its own listener. Start it after registration. Its fresh
meta ingress opens only after a current quorum supplies a durable vote floor;
metadata catches up before its boot and WAL identities register. Use `MoveReplicas`
to prepare data learners, certify catch-up and place data on the new node.

The meta credential authorizes peer replication and control writes. Every peer
must load the same secret from `raft.meta.auth_token_file`; treat possession of
this credential as cluster administrator access, keep port 4439 private, and do
not expose the secret in config repositories or logs.

## Whole-volume replacement

A fresh meta replica in an explicitly retired `RebuildReplica` operation keeps
its metadata ingress and elections closed until a
current surviving meta quorum supplies a fresh leader ReadIndex confirmation and
a durable vote floor. It persists the floor before participating. Retaining the
replacement as a meta learner allows catch-up before its startup claim and data
HTTP listener become ready. Genesis requires an all-peers-empty handshake bound
to fresh process nonces. Configuration alone cannot restore an initialized
cluster after all metadata disks, or the surviving quorum needed for recovery,
have been lost.

Automatic data-group initialization is restricted to the original replica's
first admitted boot. An interrupted initial bootstrap does not authorize a later
boot to initialize an existing group from static configuration.

## Upgrading an older storage layout

In-place adoption of pre-identity WALs is unsupported. Startup rejects an
existing journal without its durable replica identity; it must not rewrite
artifact versions, synthesize identity markers, or modify journal data to bypass
that refusal. This release provides no automatic migration from those prerelease
layouts. Preserve the original volumes and cold objects; do not delete them to
bypass startup validation. Matching `FORMAT_EPOCH` alone does not establish compatibility among
unreleased builds.

Current identity-bearing WALs support ordinary same-volume restart, including a
coordinated stop and restart of all nodes. Persistent replica identities and
placements remain unchanged while boot process epochs advance. This does not
convert an older storage format.

### Durable replica identity migration

Data RPC admission uses a persistent replica identity belonging to the data WAL
lifetime. Ordinary process restarts retain that identity; maintenance process
claims still use a new boot incarnation. The identity and its required marker
are fsynced under `raft-log` before data startup, including with WAL fsync policy
`never`. A marked WAL with a missing or corrupt identity fails closed.

A pre-identity WAL is rejected without modification. Missing identity on a marked
WAL is corruption, and whole-volume replacement uses the explicit rebuild
protocol. Neither case can be repaired by an adoption configuration flag.

A replacement starts its meta listener and registers its persisted token, then
waits for the surviving data groups to durably install their replacement fences
and for meta activation before creating data actors. Activation seeds certified
identity maps and required applied-prefix indices but does not bypass the normal
unknown-history vote-floor and current-leader recovery barrier.
