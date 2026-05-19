# EC2 Static Cluster Helper

`scripts/ursula_ec2.py` is a small ops helper for existing EC2 instances. It is
not a cluster provisioner: it assumes instances, networking, IAM, and the
`ursula-http` binary already exist. Its job is to make the common static
multi-Raft deployment loop reproducible:

- inject a short-lived EC2 Instance Connect SSH key
- start or stop one Ursula process per server instance
- wait until all Raft groups have leaders
- inspect `/__ursula/metrics`
- run `perf_compare` from an optional client host
- clean a configured S3 cold-root prefix

The shape intentionally mirrors the current migration benchmarks: three server
nodes, one optional client node, static Raft peer URLs, per-group membership
initializers, and optional S3 cold storage.

## Requirements

- AWS CLI configured with permission for `ec2-instance-connect:SendSSHPublicKey`
- SSH access through EC2 Instance Connect for the configured `ssh_user`
- server instances can reach each other on the configured Ursula port
- server instances can access the configured S3 bucket when `cold_env` uses S3
- `ursula-http` is already present and executable at `binary` on every server
- optional `perf_compare` is already present and executable on the client host

## Manifest

```json
{
  "ssh_user": "ec2-user",
  "port": 4491,
  "binary": "/tmp/ursula-http",
  "pid_prefix": "/tmp/ursula",
  "log_prefix": "/tmp/ursula",
  "core_count": 16,
  "raft_group_count": 64,
  "raft_memory": true,
  "init_membership_per_group": true,
  "cold_env": {
    "URSULA_COLD_BACKEND": "s3",
    "URSULA_COLD_S3_BUCKET": "my-ursula-bucket",
    "URSULA_COLD_S3_REGION": "us-east-1",
    "AWS_REGION": "us-east-1",
    "URSULA_COLD_ROOT": "ursula-test-20260518T000000Z",
    "URSULA_COLD_FLUSH_INTERVAL_MS": "200",
    "URSULA_COLD_FLUSH_MIN_HOT_BYTES": "1048576",
    "URSULA_COLD_FLUSH_MAX_BYTES": "1048576",
    "URSULA_COLD_FLUSH_MAX_CONCURRENCY": "32",
    "URSULA_COLD_MAX_HOT_BYTES_PER_GROUP": "67108864"
  },
  "nodes": [
    {
      "id": 1,
      "name": "node1",
      "instance_id": "i-...",
      "az": "us-east-1a",
      "public_ip": "203.0.113.10",
      "private_ip": "10.0.1.10"
    },
    {
      "id": 2,
      "name": "node2",
      "instance_id": "i-...",
      "az": "us-east-1b",
      "public_ip": "203.0.113.11",
      "private_ip": "10.0.2.10"
    },
    {
      "id": 3,
      "name": "node3",
      "instance_id": "i-...",
      "az": "us-east-1c",
      "public_ip": "203.0.113.12",
      "private_ip": "10.0.3.10"
    }
  ],
  "client": {
    "name": "client",
    "instance_id": "i-...",
    "az": "us-east-1a",
    "public_ip": "203.0.113.20",
    "private_ip": "10.0.1.20"
  },
  "perf_compare": "/tmp/perf_compare"
}
```

Do not commit manifests containing real public IPs, instance ids, or bucket
names. Keep deployment manifests outside the repository tree.

## Commands

Start the cluster:

```bash
python3 scripts/ursula_ec2.py --config /path/to/cluster.json upload-binary \
  --target servers \
  --local ./target/release/ursula-http \
  --remote /tmp/ursula-http

python3 scripts/ursula_ec2.py --config /path/to/cluster.json start
python3 scripts/ursula_ec2.py --config /path/to/cluster.json wait-ready
```

Upload a benchmark client binary:

```bash
python3 scripts/ursula_ec2.py --config /path/to/cluster.json upload-binary \
  --target client \
  --local /path/to/perf_compare \
  --remote /tmp/perf_compare
```

Inspect process state and metrics:

```bash
python3 scripts/ursula_ec2.py --config /path/to/cluster.json status
```

Run a benchmark from the configured client host. Everything after `--` is passed
to `perf_compare`; the helper supplies Ursula target URLs and the disjoint
bucket name:

```bash
python3 scripts/ursula_ec2.py --config /path/to/cluster.json perf \
  --bucket benchcmp-a -- \
  --phases write,small,mixed,read \
  --concurrency 256 \
  --throughput-secs 30 \
  --payload-bytes 128 \
  --small-payload-bytes 128 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 16 \
  --ursula-append-batch-minimal-ack \
  --mixed-appenders 128 \
  --mixed-readers 64 \
  --mixed-sse-readers 8 \
  --sse-count 100 \
  --request-timeout-secs 60 \
  --setup-concurrency 256 \
  --validate-read-len
```

Run several `perf_compare` processes concurrently from the same client host.
Each process receives a bucket named `{bucket-prefix}-{index}`, so the clients
do not contend on the same Ursula namespace. By default, the helper also rotates
the Ursula entrypoint across configured service nodes. Use
`--target-mode first` only when intentionally reproducing the older single
ingress-node shape:

```bash
python3 scripts/ursula_ec2.py --config /path/to/cluster.json perf-many \
  --processes 4 \
  --bucket-prefix benchcmp-mp \
  --remote-dir /tmp/ursula-perf-many \
  -- \
  --phases write,small,mixed,read \
  --concurrency 256 \
  --throughput-secs 30 \
  --payload-bytes 128 \
  --small-payload-bytes 128 \
  --ursula-append-mode batch \
  --ursula-append-batch-size 16 \
  --ursula-append-batch-minimal-ack \
  --mixed-appenders 128 \
  --mixed-readers 64 \
  --mixed-sse-readers 8 \
  --sse-count 100 \
  --request-timeout-secs 60 \
  --setup-concurrency 256 \
  --validate-read-len
```

The JSON outputs and per-process status files remain on the client host under
`--remote-dir` for later collection.

Stop the cluster:

```bash
python3 scripts/ursula_ec2.py --config /path/to/cluster.json stop
```

Clean a cold-root prefix:

```bash
python3 scripts/ursula_ec2.py --config /path/to/cluster.json cleanup-s3 \
  --root ursula-test-20260518T000000Z
```

## Notes

`stop` only kills the process id recorded in the configured pid file. It does
not use broad `pkill` patterns, because those can match the SSH command that is
trying to perform the cleanup.

The helper has been smoke-tested against the migration EC2 hosts without
starting Ursula: `status` successfully used EC2 Instance Connect to reach the
three server nodes and reported no running process, and `upload-binary --target
client` copied a small executable to the client host, ran it, and removed it.

The helper is intentionally small. A future production CLI should add manifest
schema validation, rolling restart, log collection, profile collection,
preflight network checks, and eventually a non-EC2 backend. This script is the
first stable surface for the EC2 benchmark/deployment loop.
