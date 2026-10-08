# Recovering a group stopped by a committed application failure

A panic or infrastructure error while applying a committed command stops that
local Raft group. Other groups remain running. This uses Rust panic unwinding;
a process abort, OOM kill or machine failure is outside this isolation boundary. The failed command receives no
successful application response, and the applied index does not advance past
it. A normal stream rejection (for example, deleting a missing stream) remains
a deterministic application result and does not stop the group.

`/__ursula/metrics` reports `raft_groups[].apply_failure` with the log term,
index, failure kind and diagnostic message. The affected group's
`maintenance.running` is false and maintenance readiness refuses another
unsafe disruption. Current node readiness also becomes unready; liveness and
other group actors remain alive. Infrastructure errors can have partially
modified memory: that state must not be read, flushed, or made into a snapshot.
A previously captured snapshot only contains its earlier successful prefix.

## Supported recovery: corrected-code replay

This procedure requires the original durable WAL and snapshot/cold objects.
It does not repair physical corruption or recover data lost beyond the chosen
WAL fsync policy. Do not truncate a committed record, edit its bytes, reset its
applied index, copy a snapshot made from failed memory, or use an unsynced-loss
operation to bypass a poison record.

1. Record the group, failing log term/index, binary revision and diagnostics on
   every replica. Stop writes to affected streams; a failed request may already
   be committed, so retain its producer sequence/idempotency information.
2. Preserve the WAL directories, snapshot pointers and referenced cold objects.
   Stop the affected server before taking a consistent filesystem backup; keep
   node IDs, membership and storage paths unchanged. Do not delete the failed
   group's data to make the pod healthy. If restarting automatically repeats
   the failure, suspend that restart loop while retaining the volumes.
3. Correct the deterministic application bug, keeping command encoding and
   semantics compatible with the retained history. Reproduce the failing
   record on an isolated copy. Verify the last acknowledged records and the
   failing command are replayed exactly once into fresh in-memory state. A
   deployment of the same faulty code is expected to stop at the same record.
4. Deploy the corrected binary to the affected replicas using their original
   volumes. A group with a live quorum can resume by normal replay/catch-up;
   if every replica stopped, bring back enough corrected replicas for quorum.
   Never create a replacement cluster or silently skip a committed command.
5. Before restoring traffic, require no `apply_failure`, running/recovery-ready
   group metrics, a current leader and applied progress through the recorded
   failing index. Read and compare previously acknowledged payloads and the
   failed request using its original identity before deciding whether to retry.

If the failure occurs during initial WAL replay, OpenRaft construction returns
`ApplyStopped`; startup may refuse to complete. Live failure isolation does not
provide a dormant failed-group handle across a restart of the faulty binary.
Use the corrected binary before resuming that server.

There is no online skip/quarantine command in this recovery contract. Repair
requires a corrected compatible binary; if none is available, keep the group
stopped and preserve its data. Rolling a snapshot or command format to an
incompatible binary is not a recovery method.

## Local drill

Run `cargo test -p ursula-raft --lib apply_failure_tests`. It uses real disk WALs
with `fsync=always`, a partial-mutation panic and an infrastructure failure,
and a three-replica in-process Raft network. It verifies stopped-group
isolation, unchanged applied boundary, retained records, same-code replay
failure, and payload preservation after corrected-code replay. The
three-replica drill delivers the original committed leader's final commit
notification explicitly so a leader failure cannot hide the poison from a
follower. This is an application-failure drill, not a power-loss or Kubernetes
Chaos qualification.
