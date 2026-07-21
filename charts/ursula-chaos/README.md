# Ursula Chaos Helm Chart

This chart installs the long-running chaos workload, verifier, status publisher, and Kubernetes fault controller against an existing Ursula release. It does not install Ursula, the gateway, the indexer, storage, or an EKS cluster.

The workload combines the existing byte-stream integrity load with JSON record streams containing `captured_at`. Record streams are registered dynamically with the target `ursula-indexer` pool. The initial Kubernetes fault profile deletes one named voter Pod at a time and waits for StatefulSet recreation plus Raft recovery before the next injection.

The Role is deliberately narrow: it can only `get` and `delete` the three voter Pod names configured under `target`. EC2-only `tc`, `iptables`, process-freeze, and S3-egress faults are not claimed as Kubernetes coverage.

When `statusS3Uri` is set, an agent restart downloads the last published object before starting. It restores the test start time, retained health history, events, workload coverage, injection history, active recovery state, and next-fault schedule. Append counters and producer state are intentionally not resumed; the new process creates a fresh workload run and removes stale index registrations owned by its Helm release. An S3 permission or transport failure aborts startup so an empty status cannot overwrite retained history; a missing object is treated as the first run.

Install Ursula first, then install this chart as a separate release:

```bash
helm install ursula charts/ursula --namespace ursula --create-namespace -f ursula-values.yaml
helm install ursula-chaos charts/ursula-chaos --namespace ursula -f chaos-values.yaml
```

The independently published OCI package is `oci://ghcr.io/tonbo-io/charts/ursula-chaos`.

For an EKS Pod Identity and a generated values file, use [`deploy/chaos-eks`](../../deploy/chaos-eks).
