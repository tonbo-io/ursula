# EC2 Static Cluster S3 Smoke

## Scope

This records short deployment-shape smokes for Ursula vNext static multi-Raft
with S3 cold storage enabled. These are not throughput benchmarks and do not
replace the official Durable Streams conformance gate.

## Current gRPC Transport Smoke

### Environment

- Time: 2026-05-18T03:40Z
- Region: `us-east-1`
- Server nodes:
  - node 1: `c7g.4xlarge`, `us-east-1a`, private IP `10.99.1.48`
  - node 2: `c7g.4xlarge`, `us-east-1b`, private IP `10.99.2.206`
  - node 3: `c7g.4xlarge`, `us-east-1c`, private IP `10.99.3.236`
- Client node: `c7gn.8xlarge`, `us-east-1a`, private IP `10.99.1.10`
- Binary: Linux aarch64 `ursula-http`, sha256
  `d48e353915876857d8a6049a202fec64286b8ecf72ce0a9a004a8d6eda1f9c9c`
- Port: `4477`, separate from the existing long-running Ursula service on
  `4466`

### Server Shape

All three servers ran the same binary with the current tonic gRPC internal Raft
transport:

```bash
/tmp/ursula-http-grpc-current \
  --listen 0.0.0.0:4477 \
  --core-count 16 \
  --raft-group-count 64 \
  --raft-memory \
  --raft-node-id <1|2|3> \
  --raft-peer 1=http://10.99.1.48:4477 \
  --raft-peer 2=http://10.99.2.206:4477 \
  --raft-peer 3=http://10.99.3.236:4477
```

Node 1 also used `--raft-init-membership`. Nodes 2 and 3 were started first.

Cold path environment:

```bash
URSULA_COLD_BACKEND=s3
URSULA_COLD_S3_BUCKET=ursula-c7g-beast-us-east-1
URSULA_COLD_S3_REGION=us-east-1
URSULA_COLD_ROOT=ursula-grpc-smoke/20260518T033351Z
URSULA_COLD_FLUSH_INTERVAL_MS=200
URSULA_COLD_FLUSH_MIN_HOT_BYTES=1
URSULA_COLD_FLUSH_MAX_BYTES=1024
URSULA_COLD_FLUSH_MAX_CONCURRENCY=8
URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=1048576
```

The launch command explicitly exported the `URSULA_COLD_*` variables before
starting the process; otherwise the sourced env file only populated shell
variables and the child process did not enable the cold store.

### Observed Result

From the dedicated client node:

- `PUT` to follower node 2 returned `307 Temporary Redirect` with
  `location: http://10.99.1.48:4477/benchcmp/grpc-smoke-cold-20260518T033351Z`
  and `x-ursula-raft-leader-id: 1`.
- `PUT` to leader node 1 returned `201 Created` with
  `stream-next-offset: 00000000000000000027`.
- A 4096-byte `POST` to leader node 1 returned `204 No Content` with
  `stream-next-offset: 00000000000000004123`. This client write requires
  cross-node gRPC AppendEntries quorum replication before commit.
- `GET` from node 3 with redirect following returned `200` and read
  `created-through-grpc-leader`.
- Node 1 metrics reached
  `cold_flush_uploads=5 cold_flush_publishes=5 cold_hot_bytes=0`.
- S3 listed five cold chunks under the smoke root: one 27-byte chunk and four
  1024-byte chunks.
- After `cold_hot_bytes=0`, a redirected read from node 3 returned the cold
  prefix and a cold range read at offset 27 returned sixteen `x` bytes.
- Node logs had no `error`, `panic`, `StreamGone`, `StreamNotFound`, or
  `cold flush worker` lines; the log files were empty.

After the smoke, the temporary `4477` processes were stopped and the five S3
smoke objects under `ursula-grpc-smoke/20260518T033351Z/` were deleted. The
existing `4466` service processes were left running.

### Interpretation

This proves the current static gRPC multi-Raft binary can run across the
existing EC2 c7g/c7gn topology with S3 cold offload enabled. It covers:

- real cross-AZ server-to-server Raft RPCs over the tonic gRPC transport;
- public leader redirects from followers instead of request-body proxying;
- committed leader writes requiring gRPC quorum replication;
- background cold flush to S3;
- S3-backed readback after hot bytes have been flushed.

It does not yet prove:

- persisted peer configuration;
- automatic lagging-follower snapshot transfer under real lag;
- `perf_compare` CPU saturation or long-running bounded-memory behavior.

## EC2 Official Conformance

The official Durable Streams conformance suite also passed from the
`c7gn.8xlarge` client against the same static gRPC cluster shape, targeting node
1 at `http://10.99.1.48:4477`. This run used the S3 backend with a separate root
and 1 MiB background-flush chunks:

```bash
URSULA_COLD_BACKEND=s3
URSULA_COLD_S3_BUCKET=ursula-c7g-beast-us-east-1
URSULA_COLD_S3_REGION=us-east-1
URSULA_COLD_ROOT=ursula-grpc-conformance/20260518T034603Z
URSULA_COLD_FLUSH_INTERVAL_MS=200
URSULA_COLD_FLUSH_MIN_HOT_BYTES=1048576
URSULA_COLD_FLUSH_MAX_BYTES=1048576
URSULA_COLD_FLUSH_MAX_CONCURRENCY=8
URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=67108864
```

Command from `/tmp/durable-streams-official` on the client node:

```bash
CONFORMANCE_TEST_URL=http://10.99.1.48:4477 \
pnpm exec vitest run --project server \
  packages/server/test/ursula-conformance.test.ts \
  --no-coverage --reporter=dot
```

Result:

```text
Test Files  1 passed (1)
Tests  300 passed (300)
Duration  20.36s
```

Post-run node 1 metrics:

```text
accepted_appends=1093
active_cores=16
active_groups=64
cold_flush_uploads=0
cold_flush_publishes=0
cold_hot_bytes=112587
```

The conformance run used the S3 backend for the official large-payload case:
S3 listed one 10 MiB object under the conformance root. Background flush did not
run because the remaining hot backlog was below the 1 MiB flush threshold. Node
logs had no `error`, `panic`, `StreamGone`, `StreamNotFound`, or
`cold flush worker` lines. After the run, the temporary `4477` processes were
stopped and the S3 conformance object was deleted.

## EC2 Durable-Log S3 Restart Smoke

### Environment

- Time: 2026-05-18T06:44Z
- Region: `us-east-1`
- Server nodes:
  - node 1: `c7g.4xlarge`, private IP `10.99.1.48`
  - node 2: `c7g.4xlarge`, private IP `10.99.2.206`
  - node 3: `c7g.4xlarge`, private IP `10.99.3.236`
- Client node: `c7gn.8xlarge`, private IP `10.99.1.10`
- Binary: Linux aarch64 `ursula-http`, sha256
  `99d29c53ea1b51a52ea4df03df9e87fade99ff1f55eb04c0665bb930f87711b8`
- Port: `4478`, separate from the existing `4466` service and the earlier
  `4477` smokes.

The binary was built on node 1 from the current Ursula workspace tarball because
the previous `/tmp/ursula-http-grpc-current` binary did not yet support static
gRPC with `--raft-log-dir`.

### Server Shape

All three servers ran with the current tonic gRPC internal Raft transport,
independent durable OpenRaft log roots, and real S3 cold storage:

```bash
/tmp/ursula-http-current-logdir \
  --listen 0.0.0.0:4478 \
  --core-count 16 \
  --raft-group-count 64 \
  --raft-log-dir /tmp/ursula-ec2-durable-s3-20260518T064426Z-durable-s3/node-<id> \
  --raft-node-id <1|2|3> \
  --raft-peer 1=http://10.99.1.48:4478 \
  --raft-peer 2=http://10.99.2.206:4478 \
  --raft-peer 3=http://10.99.3.236:4478
```

Node 1 used `--raft-init-membership` for the initial start only. The restart
phase reused the same log roots and omitted `--raft-init-membership` on every
node.

Cold path environment:

```bash
URSULA_COLD_BACKEND=s3
URSULA_COLD_S3_BUCKET=ursula-c7g-beast-us-east-1
URSULA_COLD_S3_REGION=us-east-1
URSULA_COLD_ROOT=ursula-grpc-durable-s3/20260518T064426Z-durable-s3
URSULA_COLD_FLUSH_INTERVAL_MS=200
URSULA_COLD_FLUSH_MIN_HOT_BYTES=1
URSULA_COLD_FLUSH_MAX_BYTES=1024
URSULA_COLD_FLUSH_MAX_CONCURRENCY=8
URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=1048576
```

Temporary AWS SSO credentials were exported into remote-only env files for the
test and deleted during cleanup.

### Observed Result

The client wrote a stream through public HTTP. The stream's Raft owner was node
3, so node 1 returned a `307` leader redirect and the write completed on node 3:

```text
PUT 201 http://10.99.3.236:4478/... 00000000000000000031
POST 204 http://10.99.3.236:4478/... 00000000000000004127
```

Node 3 metrics before restart showed the durable/S3 path was active and fully
drained hot bytes:

```text
cold_hot_bytes=0
cold_flush_uploads=5
cold_flush_publishes=5
wal_batches=285
wal_records=285
wal_sync_ns=141833
accepted_appends=1
```

S3 listed five chunks under the temporary root: one 31-byte create chunk and
four 1024-byte append chunks, totaling 4127 bytes.

After stopping all three `4478` processes, restarting all three nodes from the
same durable log roots without initial membership, and reading through node 1,
the request redirected to node 2 and returned the expected cold-backed prefix:

```text
read_after_restart 200 http://10.99.2.206:4478/... \
  b'created-through-ec2-durable-s3-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
```

Post-restart metrics on all three nodes showed reopened durable journals:

```text
10.99.1.48 wal_batches=192 wal_records=192 cold_hot_bytes=0
10.99.2.206 wal_batches=354 wal_records=354 cold_hot_bytes=0
10.99.3.236 wal_batches=290 wal_records=290 cold_hot_bytes=0
```

### Cleanup

The temporary `4478` processes were stopped, remote temporary log roots and
credential env files were deleted, and the S3 prefix
`ursula-grpc-durable-s3/20260518T064426Z-durable-s3/` was removed recursively.

### Interpretation

This closes the previous gap for multi-node EC2 validation with durable
OpenRaft logs and S3 enabled. It covers quorum-replicated writes, S3 cold
offload, replicated cold manifest persistence, full process restart without
reinitializing membership, and follower readback from the restarted cluster.
It is still a short correctness smoke, not a throughput or bounded-memory soak.

## EC2 Durable-Log S3 Late-Learner Snapshot Smoke

### Environment

- Time: 2026-05-18T07:25Z
- Region: `us-east-1`
- Server nodes:
  - node 1: `c7g.4xlarge`, private IP `10.99.1.48`
  - node 2: `c7g.4xlarge`, private IP `10.99.2.206`
  - node 3: `c7g.4xlarge`, private IP `10.99.3.236`
- Client node: `c7gn.8xlarge`, private IP `10.99.1.10`
- Binary: Linux aarch64 `ursula-http`, sha256
  `99d29c53ea1b51a52ea4df03df9e87fade99ff1f55eb04c0665bb930f87711b8`
- Port: `4480`

### Server Shape

Nodes 1 and 2 first ran as a two-voter static gRPC Raft cluster with
independent durable OpenRaft log roots and real S3 cold storage:

```bash
/tmp/ursula-http-current-logdir \
  --listen 0.0.0.0:4480 \
  --core-count 16 \
  --raft-group-count 1 \
  --raft-log-dir /tmp/ursula-ec2-late-snapshot-20260518T072547Z-late-snapshot-clean/node-<id> \
  --raft-node-id <1|2> \
  --raft-peer 1=http://10.99.1.48:4480 \
  --raft-peer 2=http://10.99.2.206:4480
```

Node 1 used `--raft-init-membership`; node 2 did not. After the leader
snapshotted and purged its log, node 3 started from an empty durable log root
with peers 1, 2, and 3 configured, and node 1 added it as a learner through:

```text
POST /__ursula/raft/0/learners/3?addr=http://10.99.3.236:4480
```

Cold path environment used the S3 root
`ursula-grpc-late-snapshot/20260518T072547Z-late-snapshot-clean` with
1 KiB flush chunks and `URSULA_COLD_FLUSH_MIN_HOT_BYTES=1`.

### Observed Result

The client wrote a 4136-byte stream through node 1. Node 2 read the replicated
payload before the leader snapshot:

```text
PUT 201 ... stream-next-offset=00000000000000000040
POST 204 ... stream-next-offset=00000000000000004136
node2_read_before_snapshot 200 ... b'created-through-ec2-clean-late-snapshot-...'
```

Node 1 flushed all hot bytes to S3 before snapshotting:

```text
cold_hot_bytes=0
cold_flush_uploads=5
cold_flush_publishes=5
wal_batches=11
wal_records=11
```

The leader admin endpoints then produced and purged snapshot index 4:

```text
snapshot 200 {"raft_group_id":0,"snapshot_index":4}
purge 200 {"raft_group_id":0,"purged_index":4}
```

After node 3 started late and was added as a learner, its metrics showed that
it caught up through OpenRaft full-snapshot transfer:

```text
"raft_groups":[{"raft_group_id":0,"node_id":3,"current_term":1,
"current_leader":1,"last_log_index":5,"committed_index":5,
"last_applied_index":5,"snapshot_index":4,"purged_index":4,
"voter_ids":[1,2],"learner_ids":[3]}]
```

A public HTTP read through node 3 returned the restored cold-backed stream
prefix:

```text
late_read 200 http://10.99.3.236:4480/benchcmp/... \
  b'created-through-ec2-clean-late-snapshot-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
```

S3 listed five cold chunks under the temporary root, totaling 4136 bytes.
Post-run metrics were:

```text
n1 wal_batches=14 wal_records=14 cold_flush_uploads=5 cold_hot_bytes=0 snapshot_index=4 purged_index=4
n2 wal_batches=12 wal_records=13 cold_flush_uploads=0 cold_hot_bytes=0 learner_ids=[3]
n3 wal_batches=4 wal_records=4 cold_flush_uploads=0 cold_hot_bytes=0 snapshot_index=4 purged_index=4
```

Node logs had no `error`, `panic`, `StreamGone`, `StreamNotFound`, or
`cold flush worker` lines.

### Cleanup

The temporary `4480` processes were stopped, remote temporary log roots and
credential env files were deleted, and the S3 prefix
`ursula-grpc-late-snapshot/20260518T072547Z-late-snapshot-clean/` was removed
recursively.

### Interpretation

This closes the previous EC2 gap for restart-level lagging-follower snapshot
exercise. It covers a real late node with an empty durable log, a leader whose
log was already purged to its snapshot, OpenRaft full-snapshot transfer over
the tonic gRPC Raft transport, S3 cold-backed snapshot state, and public read
from the late learner after catch-up.

It is still a short correctness smoke, not dynamic membership management or a
long-running bounded-memory/performance soak.

## Historical HTTP Transport Smoke

### Environment

- Time: 2026-05-18T01:20:42Z
- Region: `us-east-1`
- Server nodes:
  - node 1: `c7g.4xlarge`, `us-east-1a`, private IP `10.99.1.48`
  - node 2: `c7g.4xlarge`, `us-east-1b`, private IP `10.99.2.206`
  - node 3: `c7g.4xlarge`, `us-east-1c`, private IP `10.99.3.236`
- Client node: `c7gn.8xlarge`, `us-east-1a`, private IP `10.99.1.10`
- Binary: Linux aarch64 `ursula-http`, sha256
  `b1be43e437a1dd7d07130083025594301065cdbf0e53015fba3f7a3ddaad887f`
- Port: `4477`, separate from the existing riverrun service port

### Server Shape

All three servers ran the same binary with:

```bash
/tmp/ursula-http-vnext \
  --listen 0.0.0.0:4477 \
  --core-count 16 \
  --raft-group-count 64 \
  --raft-memory \
  --raft-node-id <1|2|3> \
  --raft-peer 1=http://10.99.1.48:4477 \
  --raft-peer 2=http://10.99.2.206:4477 \
  --raft-peer 3=http://10.99.3.236:4477
```

Node 1 also used `--raft-init-membership`. Nodes 2 and 3 were started first so
their Raft RPC handles were ready before node 1 initialized membership.

Cold path environment:

```bash
URSULA_COLD_BACKEND=s3
URSULA_COLD_S3_BUCKET=ursula-c7g-beast-us-east-1
URSULA_COLD_S3_REGION=us-east-1
URSULA_COLD_ROOT=ursula-vnext-smoke/20260518T011720Z
URSULA_COLD_FLUSH_INTERVAL_MS=200
URSULA_COLD_FLUSH_MIN_HOT_BYTES=1
URSULA_COLD_FLUSH_MAX_BYTES=1024
URSULA_COLD_FLUSH_MAX_CONCURRENCY=8
URSULA_COLD_MAX_HOT_BYTES_PER_GROUP=1048576
```

### Observed Result

From the dedicated client node:

- `PUT` to follower node 2 returned `201` with
  `stream-next-offset: 00000000000000000025`.
- A retrying `GET` from node 3 read `created-through-ec2-proxy`, proving
  follower write proxying plus cross-node replication was protocol-visible in
  the old HTTP transport shape.
- `POST` to node 1 returned `204` with
  `stream-next-offset: 00000000000000004121`.
- Node 1 metrics reached
  `cold_flush_uploads=5 cold_flush_publishes=5 cold_hot_bytes=0`.
- S3 listed five cold chunks under the smoke root: four 1024-byte chunks plus
  one 25-byte chunk.
- A final `GET` from node 3 returned `200` and read the expected prefix from
  the stream after cold flush.

After the smoke, the temporary `4477` processes were stopped, temporary remote
credential env files were removed, and the S3 smoke objects plus binary
artifact were deleted.

### Interpretation

This proved the then-current static HTTP multi-Raft binary could run across the
existing EC2 c7g/c7gn topology with S3 cold offload enabled. It covered:

- real cross-AZ server-to-server Raft RPCs;
- public write proxying from a follower to the leader in the old transport;
- replicated readback from another follower;
- background cold flush to S3;
- cold-backed readback after hot bytes have been flushed.

The current static multi-Raft transport uses tonic gRPC for node-to-node Raft
RPCs and returns public leader redirects instead of proxying write bodies
through followers. Repeat this EC2 smoke before citing it as current transport
evidence.

It does not yet prove:

- persisted peer configuration;
- automatic lagging-follower snapshot transfer under real lag;
- official Durable Streams conformance against the EC2 cluster;
- `perf_compare` CPU saturation or long-running bounded-memory behavior.
