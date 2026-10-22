# Ursula Chaos Helm Chart

This chart installs the long-running chaos workload, verifier, status publisher, and Kubernetes fault controller against an existing Ursula release. It does not install Ursula, the gateway, storage, or an EKS cluster.

The workload combines the existing byte-stream integrity load with JSON record streams, kept so the byte and offset checks also cover `application/json` streams. The initial Kubernetes fault profile deletes one named voter Pod at a time and waits for StatefulSet recreation plus Raft recovery before the next injection.

The `placement` fault profile changes the placement under the same load: it moves a third of the groups one voter each, kills each role of a move with a Pod deleted without a grace period (the source, the target, the group's leader, the meta leader), and fills the node holding the fewest replicas with up to 64 groups. Each injection records the change's measures: time per move and per teardown, elections per move, and the client-observed write p99 of the groups not moving, of the moving ones and before the change. The agent reads every target from `GET /__ursula/placement` and writes moves over the admin plane, so the Ursula release must open `server.adminListen` to the pod network (`0.0.0.0:4438`), which suits a test cluster only. Moves need more nodes than the replication factor.

The Role is deliberately narrow: it can only `get` and `delete` the three voter Pod names configured under `target`. EC2-only `tc`, `iptables`, process-freeze, and S3-egress faults are not claimed as Kubernetes coverage.

When `statusS3Uri` is set, an agent restart downloads the last published object before starting. It restores the test start time, retained health history, events, workload coverage, injection history, active recovery state, and next-fault schedule. Append counters and producer state are intentionally not resumed; the new process creates a fresh workload run. An S3 permission or transport failure aborts startup so an empty status cannot overwrite retained history; a missing object is treated as the first run.

Install Ursula first, then install this chart as a separate release:

```bash
helm install ursula charts/ursula --namespace ursula --create-namespace -f ursula-values.yaml
helm install ursula-chaos charts/ursula-chaos --namespace ursula -f chaos-values.yaml
```

The independently published OCI package is `oci://ghcr.io/tonbo-io/charts/ursula-chaos`.

When `statusS3Uri` is set, the chart's service account needs read and write access to that S3 object, for example through an IRSA role in `serviceAccount.annotations` or EKS Pod Identity.
