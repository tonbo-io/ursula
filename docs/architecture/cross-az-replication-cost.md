# Cross-AZ replication cost

Status: investigation for 0.4; this document does not change the durability contract.

## Goal

Reduce the complete Workflow backend cost below `$0.163 / 1M` steps while preserving lower P99 than PostgreSQL and the existing guarantee that an acknowledged append survives loss of any one availability zone.

The 0.3.32 EKS measurement attributes approximately `$0.1534 / 1M` steps to cross-AZ transfer, versus `$0.0509` for allocated compute and node storage. Server throughput work cannot remove this variable-cost floor. The next design variable is bytes sent across availability zones per acknowledged append.

## Measurement contract

`/__ursula/metrics` exposes pre-compression logical protobuf counters:

- `raft_grpc_append_heartbeat_request_bytes`;
- `raft_grpc_append_replication_request_bytes`;
- `raft_grpc_append_response_bytes`;
- `raft_grpc_vote_request_bytes` and `raft_grpc_vote_response_bytes`;
- `raft_grpc_snapshot_request_bytes`, `raft_grpc_snapshot_payload_bytes`, and `raft_grpc_snapshot_response_bytes`.

These are not AWS-billed wire bytes. The benchmark must collect the counters on all voters and compare their deltas with source-side VPC packet counters and CUR usage. The ratios answer three separate questions:

1. application amplification = replication request bytes / workload payload bytes;
2. transport amplification = source-side wire bytes / logical protobuf bytes;
3. billing amplification = charged regional bytes / source-side wire bytes.

Loaded and idle windows must be reported separately. A valid result includes per-path voter-to-voter, voter-to-gateway, and gateway-to-voter bytes.

## Current lower bound

With one full Raft entry on each of three voters, a leader sends the application payload to two remote availability zones. ZSTD and transport-frame batching reduce representation and framing overhead, but they do not change this two-copy network lower bound.

The measured 0.3.32 batching result remains worth keeping: total cross-AZ bytes fell from `9.611` to `7.669 GB / 1M` steps. It does not make another full replica free.

## Candidate A: two data voters and one metadata witness

The attractive shape is:

```text
data voter A ── full payload ──> data voter B
      └──────── digest/log metadata ──> witness C
```

This cannot be implemented by marking an ordinary OpenRaft voter as a witness. Standard Raft requires every voter log to contain the same entry, and a normal majority could commit `A + C` before B owns the payload. Ursula must not acknowledge that write: loss of A would lose an acknowledged payload.

A correct implementation needs all of these invariants:

- only a data-bearing node may lead a stream group;
- an acknowledged append exists in full in at least two availability zones;
- the witness cannot satisfy the data-availability quorum;
- elections never choose a node missing any committed payload;
- snapshot install restores the same data-availability state as log replication.

The simple `A + B data, C witness` form halves payload transfer but weakens write availability. Loss of either data AZ leaves only one full copy, so writes must pause until a replacement data voter catches up. Loss of the witness AZ still permits A and B to write. This is not equivalent to the current single-AZ-failure availability contract and must not be shipped under the same topology label.

## Candidate B: replicated metadata plus erasure-coded payload

A stronger three-AZ design keeps the compact command/digest in Raft and moves large payload bytes to a separate replicated payload plane:

```text
leader A: full hot payload
voter B: fragment B
voter C: complementary fragment C
Raft log on A/B/C: payload digest, length, and fragment manifest
```

The cross-AZ payload approaches one full copy instead of two. After losing A, B and C can reconstruct the payload. However, surviving any single-AZ loss at acknowledgement time requires both remote fragments to be durable before the client is acknowledged. That waits for the slower remote AZ and requires coordinated payload garbage collection, snapshot transfer, leader change, and repair. Applying a manifest before its fragments are durable is forbidden.

This preserves the desired failure domain more closely than a witness, but it is a new storage architecture rather than a Raft transport optimization.

## Candidate C: change the AWS tariff

PrivateLink can replace ordinary EC2 cross-AZ data-transfer charges with endpoint data-processing charges for eligible paths. It does not reduce bytes and it adds endpoint, load balancer, and processing costs. A proof of concept is valid only if:

- voter identity is preserved without cross-zone load-balancer forwarding;
- EC2 regional data-transfer usage disappears from CUR for the test path;
- endpoint, NLB, and LCU charges are included;
- commit P99 and reconnect behavior do not regress.

Transit Gateway and a normal cross-zone NLB do not meet the cost goal.

## Decision sequence

1. Deploy the logical byte counters without changing replication semantics.
2. Run equal-window idle and loaded benchmarks and attribute the remaining `7.669 GB / 1M`.
3. Run an isolated PrivateLink Raft-transport proof of concept; reject it if the NLB performs a cross-zone hop.
4. If the post-PrivateLink network cost remains above `$0.10 / 1M`, prototype the payload-plane boundary before choosing witness or erasure coding.
5. Do not implement a witness directly inside the current OpenRaft log store. First prove the payload manifest, availability quorum, election eligibility, repair, and snapshot invariants in deterministic simulation.

## Release gates

- acknowledged appends survive loss of any one AZ;
- no stale or incomplete node can become leader;
- the loaded and idle wire-byte reductions are each reproducible in three runs;
- total network cost is below `$0.10 / 1M` steps as an intermediate gate and below `$0.05 / 1M` as the target;
- Workflow P99 does not regress;
- a 0.3.32 cluster can be gracefully advanced to the 0.4 transport before removal of the unary upgrade bridge.
