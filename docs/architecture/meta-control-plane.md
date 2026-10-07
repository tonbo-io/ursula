# Meta Raft membership and maintenance

Meta Raft stores desired data-group placement, process incarnations and epochs,
and the single active maintenance operation. Its journal and snapshots are under
`<raft.wal.path>/meta-raft`, separate from the per-core data journals. Metadata is
checksummed, exclusively locked, and fsynced independently of the data WAL's
`fsync` policy.

A process claims an epoch before serving data Raft traffic. Incoming data RPCs
are bound to that process identity, including each frame of an append stream.
The live placement view drives routing and engine ownership; removing a replica
from committed placement prevents the startup configuration from reopening it.

## Operations

The admin `POST /__ursula/control/operation` endpoint accepts typed intent.
`ursulactl operation --config nodes.json --node-id NODE --request intent.json`
is the platform adapter. It neither owns a Kubernetes ConfigMap lock nor accepts
caller-provided prefix evidence. The server obtains a leader ReadIndex proof and
checks the pinned replicas against that fixed prefix through their configured
private cluster endpoints.

- `MoveReplicas` prepares target engines, adds learners, changes voters and
  commits placement after evidence confirms all desired voters have caught up.
- `RebuildReplica` first proves the surviving data quorum, removes the source
  from data and meta voters while retaining it as a learner, and retires its process
  epoch. The replacement claims a new epoch after metadata catch-up. Reconciliation
  restores its meta voter role; completion requires data-group recovery too.
- `DecommissionNode` also removes the source from meta membership. Completion
  records the node as removed only after the desired data memberships are proved.
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

## Migration from a cluster without meta Raft

This is a coordinated restart with an availability gap. An ordinary ordered
rolling upgrade is not supported for this cutover: the first new pod would wait
for a meta quorum while old pods do not serve the metadata transport.

1. Stop incoming writes, verify the current cluster and retain a backup and the
   exact original persistent-volume bindings. Record stream offsets and content
   checks to verify after restart.
2. Disable automatic rollout jobs and stop all server pods cleanly. Preserve the
   data PVCs; do not delete or replace their contents.
3. Render the new chart with meta enabled, the same data group count and node
   identities, persistent WAL paths, and the complete meta peer list. Meta port
   4439 must be reachable between pods. The headless Service publishes unready
   addresses, and StatefulSet pod management must be `Parallel`.
4. Start the complete voter set together. The new metadata journals perform the
   all-empty metadata bootstrap; the existing data journals are reopened rather
   than initialized as new data groups.
5. Require serving readiness and the separate admin maintenance-readiness check,
   then verify the recorded streams and offsets before restoring traffic.

Kubernetes serving readiness checks local initialized, recovered voter replicas,
catch-up, disk watermarks and format epoch. Remote membership completeness is
checked separately at `/__ursula/maintenance/ready`; a healthy surviving quorum
therefore remains in Service endpoints while a rebuilding learner stays unready.
