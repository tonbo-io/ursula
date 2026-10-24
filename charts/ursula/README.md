# Ursula Helm Chart

This chart installs Ursula as a Raft cluster on Kubernetes whose meta group places every Raft group. The default install starts three server pods, 64 Raft groups, durable per-pod Raft log PVCs, a headless peer Service, an internal client/admin ClusterIP Service, a quorum-protecting PodDisruptionBudget, default multi-pod spread hints, an optional stateless gateway Deployment, and a Helm test that verifies cluster readiness with `ursulactl wait-ready`.

The meta group seeds each Raft group once, at bootstrap, on up to three pods. The StatefulSet updates by `RollingUpdate`, one pod at a time, each waiting for serving readiness, and every voter hands off its leaderships on `SIGTERM`. A replica that lost Raft log entries is gated, stays a voter, and is caught up by its group leaders without an operator. Raising `server.replicaCount` adds pods that hold no group (see [Scaling](#scaling-and-serverreplicacount)).

## Recommended Production Topology

- Run three Ursula server pods across three availability zones for the normal production profile. Each Raft group has at most three voters whatever the pod count, so more pods do not tolerate more simultaneous failures.
- Every voter gets its own zonal persistent volume for the Raft log, and the cluster uses shared S3 for cold chunks and externalized snapshots.
- Run at least two stateless gateway replicas behind an authenticated TLS ingress or load balancer. Keep the server and peer Services private.
- Give Ursula a workload identity with least-privilege access to its S3 prefix. Enable bucket versioning, encryption, lifecycle policy, access logging, and alerts for S3 errors, storage growth, and pod restarts.

This topology removes single-pod compute failures from the normal request path. Persistent Raft groups make membership initialization idempotent across restarts. `helm upgrade` rolls the voters with `RollingUpdate`: the controller replaces the highest ordinal first and moves to the next pod only once the replaced one passes its readiness probe, which waits until the pod's voter replicas caught up. A pod the controller recreates during the roll, after a crash or an eviction, takes the new revision. On Kubernetes 1.37 and later, the controller first replaces every pod that is already unready on the old revision, all at once, and replaces a ready pod only while every other pod is ready. Each replaced voter hands off its leaderships on `SIGTERM` before it stops. The controller does not look at the other pods, so start an upgrade only when `ursulactl verify-cluster` passes.

[`examples/production-eks.yaml`](examples/production-eks.yaml) is a concrete three-AZ starting point with durable gp3 volumes, S3 cold storage and snapshots, three gateways, and an IRSA workload identity. It needs the AWS resources listed under [Prerequisites](#prerequisites).

[`examples/tls-ingress.yaml`](examples/tls-ingress.yaml) is the exposure half: one HTTPS domain terminating at the ingress, with voters and the admin plane private. Layer it on top of a sizing example. Ursula never terminates TLS itself — see the security guide for why, and for the ingress settings that live tails and leader redirects depend on.

## Prerequisites

The chart needs Kubernetes 1.24 or later and a StorageClass for the per-pod Raft
log PVCs. A multi-node cluster also needs an S3 or S3-compatible bucket for cold
chunks and snapshots.

On AWS, [`examples/production-eks.yaml`](examples/production-eks.yaml) expects:

- An EKS cluster with nodes in three availability zones. The example spreads
  voters and gateways across zones with `whenUnsatisfiable: DoNotSchedule`.
- The EBS CSI driver add-on and a StorageClass named `gp3`, for example with
  provisioner `ebs.csi.aws.com`, parameters `type: gp3` and `encrypted: "true"`,
  `volumeBindingMode: WaitForFirstConsumer` and `reclaimPolicy: Retain`.
- An S3 bucket in the cluster's region with versioning, default encryption and
  blocked public access. Set `s3.bucket`, `s3.region` and `s3.prefix`.
- An IAM role for the Ursula service account, through IRSA (the
  `eks.amazonaws.com/role-arn` annotation in the example) or EKS Pod Identity.
  Limit the role to `s3.prefix`. It needs `s3:ListBucket`,
  `s3:GetBucketLocation` and `s3:ListBucketMultipartUploads` on the bucket for
  its prefix, and `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`,
  `s3:AbortMultipartUpload` and `s3:ListMultipartUploadParts` on the objects
  under it.

## Build A Local Image

```bash
docker build -t ursula:dev .
```

For kind:

```bash
kind load docker-image ursula:dev
```

## Install

From the published OCI chart:

```bash
helm install ursula oci://ghcr.io/tonbo-io/charts/ursula --version 0.10.0 \
  --set server.clusterId=my-cluster
```

For a local image loaded into the cluster:

```bash
helm install ursula charts/ursula \
  --set server.clusterId=dev-local \
  --set global.image.repository=ursula \
  --set global.image.tag=dev \
  --set global.image.pullPolicy=Never
```

For a registry image, set `global.image.repository`, `global.image.tag`, and
optionally `global.imagePullSecrets`.

## Verify

```bash
helm test ursula
```

The test mounts the chart-generated `cluster-manifest.json` and runs:

```bash
ursulactl wait-ready \
  --config /etc/ursula/test/cluster-manifest.json \
  --timeout-secs 120
```

`wait-ready` succeeds only when every node's maintenance report is ready and
`GET /__ursula/ready` returns 200, and the meta leader's placement view shows
the cluster bootstrapped with every Raft group live, no move, and running on
exactly its target voters with a leader among them.

## Access Locally

```bash
kubectl port-forward svc/ursula 4437:4437
curl http://127.0.0.1:4437/__ursula/metrics
```

Operator endpoints such as bucket purge (`DELETE /__ursula/purge/{bucket}`) are
served only on the admin listener, which `server.adminListen` keeps on pod
loopback (`127.0.0.1:4438`) and no Service exposes. Reach it on one voter pod
with `kubectl port-forward` (or run `curl` inside the pod with `kubectl exec`):

```bash
kubectl port-forward pod/ursula-0 4438:4438
process_incarnation=$(curl --fail http://127.0.0.1:4438/__ursula/metrics | jq -er .process_incarnation)
curl --fail -H "x-ursula-process-incarnation: ${process_incarnation}" \
  -X DELETE http://127.0.0.1:4438/__ursula/purge/tenant-a
```

Every admin mutation needs the `x-ursula-process-incarnation` header with the
`process_incarnation` the node reported before the operation. A node answers
`428` without it and `412` after a restart.

## Expose With Ingress

Set `gateway.ingress.enabled=true` to create a Kubernetes Ingress for the
gateway Service. Keep `gateway.service.type=ClusterIP` unless your environment
requires a NodePort or LoadBalancer Service.

```yaml
gateway:
  ingress:
    enabled: true
    className: nginx
    annotations:
      cert-manager.io/cluster-issuer: letsencrypt-prod
    hosts:
      - host: ursula.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: ursula-tls
        hosts:
          - ursula.example.com
```

The chart routes all configured Ingress paths to the gateway Service and its
named Service port `gateway`.

The ingress controller must not resend a write to another gateway pod. With
ingress-nginx, set `nginx.ingress.kubernetes.io/proxy-next-upstream: "off"`,
as `examples/tls-ingress.yaml` does. A resent write can be answered as never
applied although the first attempt committed (see
[redirects and retries](https://ursula.tonbo.io/docs/api/overview#redirects-and-retries)).

In a service mesh, also turn off retries for the stream routes, both to the
gateway Service and from the gateway to the server pods. Istio before 1.24,
for example, resends a request up to twice by default when it gets a `503`,
and any version resends requests on a route whose `retryOn` includes `503`,
`5xx`, `gateway-error` or `reset`. Istio 1.24 to 1.26 restores the default
`503` retry when istiod runs with `EXCLUDE_UNSAFE_503_FROM_DEFAULT_RETRY=false`,
which `compatibilityVersion=1.23` sets. Nothing between the gateway and the
nodes may resend a request or answer `401`, `402` or `429`, as a local rate
limit does, so set `gateway.upstreams` only to URLs that reach the nodes
themselves.

## Network Planes

Each voter serves three planes. The client plane (`server.ports.client`,
`4437`) serves the stream API, and the client Service exposes only that port.
The cluster plane (`server.ports.cluster`, `4439`) carries Raft gRPC between
voters. The chart renders it as `server.cluster_listen`, and only the headless
peer Service names it. Each generated `[[raft.peers]]` entry has a `url` on the
cluster port, which peers dial, and a `client_url` on the client port, which
leader redirects name. The admin plane stays on pod loopback
(`server.adminListen`).

Set `server.clusterListen=false` to serve the client and cluster planes on the
client port, as charts before 0.8 did. Anyone who reaches the client Service can
then send Raft traffic. Changing it, or `server.ports.cluster`, later moves
every voter's cluster listener, which needs every voter restarted together.
Run that `helm upgrade` without `--wait` or `--atomic`, then delete every server
pod at once with the release's server selector labels:

```bash
kubectl delete pod -l app.kubernetes.io/instance=<release>,app.kubernetes.io/name=<chart name or nameOverride>
```

The gateway's `-gateway` name does not match that selector. The upgrade starts
the rollout itself. The controller replaces the highest ordinal, which comes up
alone on the new port and stays unready, so the rollout cannot finish on its
own. `--wait` would hang until its timeout, `--atomic` would roll the change
back, and a GitOps sync shows Progressing until the delete. Expect one voter
isolated in between. The recreated pods take the new revision, all together
under the `Parallel` pod management policy. See
[Changing a peer address](https://ursula.tonbo.io/docs/operations#changing-a-peer-address).

A Service does not stop a pod from dialing another pod's IP. To restrict that,
enable the NetworkPolicy on a CNI that enforces it:

```yaml
server:
  networkPolicy:
    enabled: true
    # Empty admits every source to the client port.
    clientIngressFrom:
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: apps
```

The policy admits only this release's server pods to the cluster port. The
client port admits `clientIngressFrom`, plus this release's gateway, Helm
test and scale-check pods. List every other client there, including metrics pollers
and an `ipBlock` for traffic through a `LoadBalancer` or `NodePort` Service.
The chart refuses to render the policy while `server.adminListen` is off
loopback, since the policy would cut that admin plane off. It selects only
ingress, so egress stays open. Kubelet probes reach the client port from the
node, which most CNIs admit whatever the policy says.

## Bootstrap Behavior

At bootstrap the meta group seeds each Raft group on three consecutive pods in node id order, starting at the pod of index `group mod replicaCount`, so a three-pod cluster runs every group on every pod. The first pod of a group's window initializes it, and only once every voter of the group reports it empty and no leader. A replica that ever held its group never initializes it again, so restarts and upgrades need no setting. The placement lives in the meta group from then on, so a later `server.replicaCount` changes no group's voters.

### Raft WAL fsync policy and recovery

`raft.walFsync` chooses when Raft WAL appends reach stable storage. Unset, the default, the server takes `always` when each group has one or two voters (a chart of one pod) and `never` otherwise. Under `never` a process crash loses nothing, but a host crash or power loss can cost a replica the unsynced tail of its journals. Such a replica restarts behind a recovery gate for every group it held: it does not campaign or vote until it has applied a fresh barrier from the group's leader, and the leader catches it up in place. Readiness answers `503` with reason `recovery_gate_closed` meanwhile. While at most a minority of a group's voters lost their tail, no acknowledged write is lost.

If a majority of a group's voters lose their tail at once, the group has no leader and refuses writes, and the gated replicas report it stalled (readiness reason `recovery_stalled`). It resumes only after an operator accepts the loss of writes acknowledged after the last `fsync` with a recovery plan, sent with `ursulactl recover-group --accept-loss` or through `POST /__ursula/raft/{target}/recovery/accept-unsynced-loss` to the plan's leader first and then to the other voters. Recover the meta group first (`--target meta`), then the data groups. Set `raft.walFsync=always` to acknowledge every batch after `fsync` instead. See the [operations guide](https://ursula.tonbo.io/docs/operations#recovering-after-a-host-crash).

These recovery mechanisms do not by themselves establish production qualification: test the chosen topology under single-voter loss, host crashes, delayed replication, snapshot-store failures, and production memory limits. Serving readiness (`/__ursula/ready`) checks the groups the node's placement view gives it, running local voter replicas, recovery gates, applied membership and bounded local lag. While a voter that lost entries catches up, healthy surviving voters remain Service endpoints, while the catching-up voter stays NotReady. With three pods and the default PDB, that unavailable voter consumes the one allowed disruption, and a `RollingUpdate` does not move past it. `ursulactl wait-ready` combines the serving probe with each node's maintenance report and every group checked on its target voters in the meta leader's view. Serving readiness does not continuously prove quorum availability or serialize separate maintenance workflows. `ursulactl verify-quorum` obtains fresh per-group confirmations and verifies application through their fixed prefixes.

## Scaling And `server.replicaCount`

`server.replicaCount` is the server pod count: `1`, or `3` and more. The chart
refuses `2`, which tolerates no node loss: every group and the meta group
would need both pods. Production clusters should use `3` or more.

Raising `server.replicaCount` on a cluster of three or more pods adds pods that
register with the meta group and hold no group: the placement keeps every
group on its voters. Move groups onto them with `ursulactl placement move`,
which stays manual.

The gateway's generated upstream list names every server pod, so any change of
`server.replicaCount` also rolls the gateway Deployment. Each terminating
gateway pod keeps serving for `gateway.preStopDelaySeconds` before it gets
`SIGTERM`, so that the Service stops sending it new connections first, and then
drains within `gateway.gracefulShutdownTimeoutSeconds`.

Node ids are the pod ordinal plus one, so a lower count drops the highest ids.
A dropped node's process and disk never take part again, but its id does: once
the meta leader completed the node's removal (every group confirmed it no
member, its snapshot objects are gone and no process of it answers), the id is
`vacant`, and a later raise starts it again on an empty volume as a new node.
The meta voters (ids 1 to 3) are never decommissioned. To lower the count:

1. Decommission every id above the new count, from a manifest that reaches
   the admin plane (see [Access Locally](#access-locally)):
   `ursulactl node decommission --config manifest.json --node 4 --node 5 --wait`.
   It cordons every named node first, so no group moves onto a node that
   leaves too, and returns once the meta leader moved every group away and
   removed each node.
2. Lower `server.replicaCount` with `helm upgrade` right after the
   decommission returns. The StatefulSet deletes the dropped pods, which lets
   each removal complete, and keeps their PVCs. A dropped pod that restarts
   before the lower count, after a removal pass ran while it was down, comes
   back superseded: unready, and holding the disruption budget until the
   count is lowered.

A raise back deletes the old PVC of each started id that is vacant, so you
need not delete them yourself. A raise binds its new ids, so the hook refuses a `helm
rollback` to the lower count until they are decommissioned again.

A single pod never grows under the chart. A single node grows only with
`raft.meta_voters` and `raft.placement.replication_factor` set on every node
([Adding a node](https://ursula.tonbo.io/docs/operations#adding-a-node)), and
the chart sets neither, so the pods of a raised count would claim the
defaults and report `replication_factor_mismatch`. To run more pods, install
a new release and restore a backup into it.

Before every upgrade and rollback the `scale-check` hook compares the live
StatefulSet's replicas with the new count. No StatefulSet and an unchanged
count pass, so an image-only upgrade always proceeds. A raise from 1 is
refused. A lower count runs `ursulactl node verify-removed` for every dropped
id. A higher count runs `ursulactl node verify-joinable` for every added id,
which waits up to 200 seconds for each removal to complete, then checks each
added ordinal's PVC that still exists with `ursulactl node verify-vacant` and
acts on its exit status alone: it deletes the PVC of a vacant id, which holds a
removed node's disk, keeps the PVC of an id in a life, so a scale-down that
skipped the hook comes back on its volumes, and refuses the raise, naming the
PVC, for anything else. The hook runs within Helm's default 5 minute
`--timeout`. It reads the live count with `kubectl`, through a ServiceAccount
allowed to `get` this release's StatefulSet and to `get` and `delete` the
`raft-data` PVCs of the ordinals a raise can start, and reaches the pods'
client plane, which the NetworkPolicy admits it to. Set
`server.scaleCheck.enabled=false` to skip it.

## Replacing A Lost Volume

A pod whose `raft-data` PVC was lost or replaced starts on an empty WAL under
its node id. The meta group still binds that id to the old volume's root, so
the pod takes part in no group and reports readiness `root_refused`. Once the
old volume is revoked (deleted, or detached with its pod gone), release it
from a manifest that reaches the admin plane:
`ursulactl node replace-disk --config manifest.json --node 3 --expect-root R`,
where `R` is the old root the pod's `GET /__ursula/placement` names. The pod
then binds its new volume with no restart, waits 12 seconds and catches up.
A release cannot be undone. Replace one pod's volume at a time, and run
`ursulactl wait --node N` before the next.

## S3 Object Storage

Cold chunks and externalized Raft snapshots share one S3-compatible object
storage configuration. Configure the bucket once under `s3`, then enable the
features that should use it.

For a TOS/S3-compatible bucket with inline credentials:

```yaml
s3:
  bucket: bj-test
  region: cn-beijing
  endpoint: https://tos-cn-beijing.ivolces.com
  prefix: ursula-dev
  credentials:
    accessKeyId: AKIA...
    secretAccessKey: ...

coldStorage:
  enabled: true

snapshotStore:
  backend: s3
```

This stores cold chunks under the resolved cold root (`s3.prefix/cold` here).
S3 snapshots use the resolved cold storage root plus `storage.snapshot.s3_prefix`
(rendered from `snapshotStore.prefix`), so this example writes snapshots under
`ursula-dev/cold/snapshots`, not directly under `ursula-dev/snapshots`. The chart
renders inline credentials into a Secret named `<release>-s3` and wires Ursula to
it through `env.valueFrom.secretKeyRef`, so the StatefulSet does not contain
plaintext credentials.

## S3 With Existing Secret

You can manage the S3 credential Secret yourself and reference it with
`s3.credentials.existingSecret`. The Secret must contain `accessKeyId` and
`secretAccessKey`; `sessionToken` is optional.

```bash
kubectl create secret generic ursula-s3 \
  --from-literal=accessKeyId=AKIA... \
  --from-literal=secretAccessKey=... \
  --from-literal=sessionToken=...
```

```yaml
s3:
  bucket: my-ursula-bucket
  region: us-east-1
  prefix: ursula-prod
  credentials:
    existingSecret: ursula-s3

coldStorage:
  enabled: true

snapshotStore:
  backend: s3
```

`s3.credentials.existingSecret` is mutually exclusive with inline
`s3.credentials.accessKeyId`, `s3.credentials.secretAccessKey`, and
`s3.credentials.sessionToken`. When an existing Secret is configured, the
StatefulSet passes `URSULA_COLD_S3_ACCESS_KEY_ID`,
`URSULA_COLD_S3_SECRET_ACCESS_KEY`, and optional `URSULA_COLD_S3_SESSION_TOKEN`
from that Secret to the entrypoint; the entrypoint writes the corresponding
`storage.cold.s3.*` values into `/etc/ursula/generated/ursula.toml` rather than
rendering the old cold-storage env-based config.

## S3 With Workload Identity

Prefer cloud workload identity over static credentials when possible:

```yaml
s3:
  bucket: my-ursula-bucket
  region: us-east-1
  prefix: ursula-prod

serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/ursula-s3

coldStorage:
  enabled: true
```

When `coldStorage.enabled=true`, the entrypoint writes generated config values to
`/etc/ursula/generated/ursula.toml`. The chart maps Helm values into canonical
Ursula config keys rather than relying on Helm-rendered `URSULA_COLD_*` process
configuration. When S3 credentials are configured, the StatefulSet still passes
`URSULA_COLD_S3_ACCESS_KEY_ID`, `URSULA_COLD_S3_SECRET_ACCESS_KEY`, and optional
`URSULA_COLD_S3_SESSION_TOKEN` from the credential Secret so the entrypoint can
write those values into the generated config file.

The default cold-read cache is installed by the runtime when no
`coldStorage.cache.*` values are set. Set `coldStorage.cache.maxSizeBytes=0` to
disable the cache explicitly.

## Snapshot Store

The default snapshot backend is `auto`, Ursula's own default: the chart renders
`storage.snapshot.backend = "auto"`, which stores Raft snapshots in S3 whenever
`coldStorage.enabled=true` and inline (inside the Raft log stores) otherwise. Pin
a backend explicitly to override that choice:

```yaml
snapshotStore:
  backend: s3     # or inline to keep snapshots out of S3 even with cold storage
```

Releases that relied on the old `inline` default and enable cold storage switch
to S3 snapshots on upgrade. That is the intended production setting; set
`snapshotStore.backend=inline` to keep the old behaviour.

When `snapshotStore.backend=s3`, the generated config sets
`storage.snapshot.backend = "s3"` (with `auto` and cold storage, the same S3
settings apply) and shares the top-level `s3` bucket/credential
settings. The snapshot driver ticks every 5 seconds by default. Set
`snapshotStore.driveIntervalMs=10000` to render
`storage.snapshot.drive_interval = "10000ms"`. The driver always runs, so the
schema refuses `0`.

It uses the shared top-level `s3` bucket/credential settings. Runtime S3 snapshot
keys are built under the resolved `storage.cold.root` plus
`storage.snapshot.s3_prefix` (rendered from `snapshotStore.prefix`). When
`coldStorage.enabled=true`, that resolved root is `s3.prefix/coldStorage.prefix`;
when cold storage is disabled while `snapshotStore.backend=s3`, the chart does not
render `storage.cold.root`, so snapshot keys are relative to the S3 bucket root
plus `snapshotStore.prefix`.

## Upgrade Limitations

Ursula 0.10 (format epoch 6) cannot read the data of a 0.9 release. Install it
fresh, as a new release with new PVCs and a new `s3.prefix` (and, with cold
storage off and S3 snapshots, a new `snapshotStore.prefix`), with the 0.9
release's `server.clusterId` and `raft.groupCount`. `helm upgrade` from 0.9
stops before a second voter is replaced, because the first new pod finds the
0.9 data on its volume and exits at startup. Recover with `helm rollback`. A
values file that still sets a value 0.10 removed (`server.updateStrategy`,
`server.podManagementPolicy`, `server.probes.readiness.enabled`,
`server.onDeleteMigration`, `server.startupOwnership`,
`server.gracefulRollout`, `raft.initMembership`, `raft.initMembershipPerGroup`)
fails validation first. Backups keep format 5, so move the data with
`ursulactl backup-create` of 0.9, a copy of the 0.9 cold objects into the new
prefix and `ursulactl restore` of 0.10. Then delete the 0.9 release's
`<StatefulSet>-rollout-state` and `<StatefulSet>-maintenance` ConfigMaps with
its other resources. See the operations guide's "Upgrading to 0.10".

The StatefulSet updates by `RollingUpdate` and never changes Raft membership.
Each replaced voter hands off its leaderships on `SIGTERM`, and the controller
moves on only once the new pod passes its readiness probe, which waits for its
voter replicas to catch up. A pod the controller recreates during the roll
takes the new revision. On Kubernetes 1.37 and later the controller first
replaces every pod already unready on the old revision, all at once. Start an
upgrade only when `ursulactl verify-cluster` passes. For a manual restart of one voter outside a rollout, run `ursulactl drain`,
restart the pod, then `ursulactl wait` and `ursulactl undrain` before the next voter.

## Values Reference

### Global

| Value | Default | Description |
| --- | --- | --- |
| `global.image.repository` | `ghcr.io/tonbo-io/ursula` | Ursula image repository. The image must contain the binaries required by the enabled chart components. |
| `global.image.tag` | `""` | Image tag. Empty uses the chart `appVersion`. |
| `global.image.pullPolicy` | `IfNotPresent` | Kubernetes image pull policy for server, gateway, and test pods. |
| `global.imagePullSecrets` | `[]` | Optional image pull secret references rendered into server, gateway, and test pods. |
| `global.clusterDomain` | `cluster.local` | Kubernetes cluster DNS domain used for generated peer FQDNs, the Helm test's cluster manifest and gateway upstreams. A change rolls like any restart while the old domain still resolves. |

### Cluster And Naming

| Value | Default | Description |
| --- | --- | --- |
| `nameOverride` | `""` | Override the chart name portion of generated resource names. Must be a lowercase DNS-1123 label when set. |
| `fullnameOverride` | `""` | Override the full generated release name. Must be a lowercase DNS-1123 label with room for generated suffixes. |

### ServiceAccount

| Value | Default | Description |
| --- | --- | --- |
| `serviceAccount.create` | `true` | Create a dedicated no-RBAC ServiceAccount. |
| `serviceAccount.name` | `""` | ServiceAccount name override, or existing ServiceAccount name when `create=false`. Must be a lowercase DNS-1123 label when set. |
| `serviceAccount.annotations` | `{}` | ServiceAccount annotations, commonly used for cloud workload identity. |
| `serviceAccount.automountServiceAccountToken` | `false` | Controls token automount for Ursula server, gateway, and test pods. |

### Server

| Value | Default | Description |
| --- | --- | --- |
| `server.replicaCount` | `3` | Server pod count: `1`, or `3` and more (`2` is refused). Raising it from `3` or more adds pods that hold no group. Lowering it needs every dropped id decommissioned first, a later raise starts those ids again as new nodes, and a single pod never grows ([Scaling](#scaling-and-serverreplicacount)). |
| `server.terminationGracePeriodSeconds` | `30` | Must exceed the server's overall 20-second shutdown budget, including up to 5 seconds for leadership handoff before Raft transport closes. |
| `server.ports.client` | `4437` | Ursula client-plane process port. The client Service, the headless peer Service, the gateway upstreams and leader redirects (each peer's `client_url`) target this port. With `server.clusterListen=false` the generated Raft peer URLs target it too. |
| `server.ports.cluster` | `4439` | Cluster-plane (Raft gRPC) process port, used when `server.clusterListen=true`. The generated Raft peer URLs and the headless peer Service target it. The client Service never exposes it. Must differ from `server.ports.client` and the `server.adminListen` port. |
| `server.clusterListen` | `true` | Serve the cluster plane on `server.ports.cluster` (`server.cluster_listen`) instead of sharing the client port. With `false`, anyone who reaches the client Service reaches Raft. A change moves every voter's cluster listener and needs every voter restarted together. |
| `server.adminListen` | `127.0.0.1:4438` | Mutating admin-plane listen address. Keep the loopback default for normal deployments and use `kubectl port-forward`; a trusted in-cluster operator may opt in to `0.0.0.0:4438`. The chart never exposes this port through a Service. |
| `server.clusterId` | `""` | The deployment's id (`server.cluster_id`), sent as `Ursula-Cluster-Id` on every `HEAD`. Required for every install, a single replica included, since the chart always renders the peer list, and rendering fails without it: name the environment and the region, such as `prod-us-east-1`, and give every deployment one client can reach its own. A node refuses to start when its WAL holds another id, so keep the old id when a restore replaces a deployment. |
| `server.service.enabled` | `true` | Render the internal client Service. |
| `server.service.type` | `ClusterIP` | Client Service type. Allowed values are `ClusterIP`, `NodePort`, and `LoadBalancer`. |
| `server.service.port` | `4437` | Client Service port. |
| `server.service.annotations` | `{}` | Client Service annotations. |
| `server.headlessService.annotations` | `{}` | Headless peer Service annotations. The headless Service targets `server.ports.client` and, with `server.clusterListen=true`, `server.ports.cluster`. It uses `publishNotReadyAddresses: true` for stable peer DNS during bootstrap. |
| `server.networkPolicy.enabled` | `false` | Render an ingress NetworkPolicy for the server pods. The cluster port admits only this release's server pods. The admin plane stays on loopback and needs no rule, and rendering fails when `server.adminListen` is off loopback. Needs a CNI that enforces NetworkPolicy. |
| `server.networkPolicy.clientIngressFrom` | `[]` | NetworkPolicy peers (`podSelector`, `namespaceSelector`, `ipBlock`) admitted to the client port. Empty admits every source. This release's gateway, Helm test and scale-check pods are always admitted when enabled. List every other client, including metrics pollers and an `ipBlock` for `LoadBalancer` or `NodePort` traffic. |
| `server.scaleCheck.enabled` | `true` | Render the `pre-upgrade,pre-rollback` hook that checks a change of `server.replicaCount` ([Scaling](#scaling-and-serverreplicacount)): a ServiceAccount, a Role allowed to `get` this release's StatefulSet and to `get` and `delete` the `raft-data` PVCs a raise can start, a ConfigMap and a Job. |
| `server.scaleCheck.activeDeadlineSeconds` | `300` | Deadline of the hook's Job. |
| `server.scaleCheck.kubectlImage` | `alpine/k8s:1.33.3` | `repository`, `tag` and `pullPolicy` of the image `kubectl` is copied from into the hook's Ursula container. |
| `server.scaleCheck.resources` | `{}` | Resources of the hook's container. |
| `server.podAnnotations` | `{}` | Extra annotations applied to Ursula server pods. |
| `server.podLabels` | `{}` | Extra labels applied to Ursula server pods. Must not set selector labels `app.kubernetes.io/name` or `app.kubernetes.io/instance`; the chart fails rendering if those keys are used. |
| `server.rustLog` | `ursula=info,ursula_runtime=info,ursula_raft=info` | `RUST_LOG` tracing filter. |
| `server.coreCount` | `4` | Ursula runtime core count. Tune this with CPU requests and limits. |
| `server.extraArgs` | `[]` | Extra CLI args appended after generated args. Use strings; with Helm CLI numeric-looking values usually require `--set-string`. |
| `server.extraEnv` | `[]` | Extra container environment entries appended after chart-managed entries. Non-secret generated TOML settings come from chart values, while explicit env entries can override environment passthrough values consumed by the entrypoint, especially S3 credential env vars. The entrypoint validates critical inputs before starting Ursula. |
| `server.extraEnvFrom` | `[]` | Extra `envFrom` blocks for server pods. |
| `server.resources` | `{}` | Container resource requests and limits for the Ursula server container. |
| `server.podSecurityContext` | `{fsGroup: 10001, fsGroupChangePolicy: OnRootMismatch}` | Pod-level securityContext for Ursula server pods. |
| `server.securityContext` | `{runAsUser: 10001, runAsGroup: 10001, runAsNonRoot: true, readOnlyRootFilesystem: true, allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}` | Container-level securityContext for the Ursula server container. |
| `server.probes.startup` | `{enabled: true, failureThreshold: 180, periodSeconds: 5, timeoutSeconds: 2}` | TCP startup probe. The 15-minute budget allows S3 snapshot restore before Kubernetes enables liveness checks. |
| `server.probes.readiness` | `{periodSeconds: 5, timeoutSeconds: 2}` | HTTP `GET /__ursula/ready` probe, always rendered since it paces `RollingUpdate`. It removes the pod from service while the WAL free-space guard is active, or after a Raft protocol (format-epoch) mismatch with a peer until the pod restarts (`format_epoch_mismatch`). |
| `server.probes.liveness` | `{enabled: true, periodSeconds: 10, timeoutSeconds: 2}` | TCP liveness probe enabled after startup succeeds. |
| `server.podDisruptionBudget.enabled` | `true` | Render a PDB for multi-node clusters. The chart omits the PDB when `server.replicaCount=1`. |
| `server.podDisruptionBudget.maxUnavailable` | `1` | Maximum voluntary disruptions, at most `1`: each Raft group has three voters, and two disruptions can take two of them. |
| `server.scheduling.nodeSelector` | `{}` | Node selector labels for server pods. |
| `server.scheduling.tolerations` | `[]` | Tolerations for server pods. |
| `server.scheduling.affinity` | `{}` | Affinity rules for server pods. Empty uses the chart's default soft hostname anti-affinity when `server.replicaCount > 1`; set a non-empty value to override. |
| `server.scheduling.topologySpreadConstraints` | `[]` | Topology spread constraints for server pods. Empty uses the chart's default zone spread hint with `ScheduleAnyway` when `server.replicaCount > 1`; set a non-empty value to override. |

The StatefulSet sets `enableServiceLinks: false`, so Kubernetes does not inject
`*_SERVICE_HOST`, `*_SERVICE_PORT`, or other Service-link variables into Ursula
pods. Apart from Kubernetes-internal variables such as `HOSTNAME`, the Ursula
container receives only chart-managed container settings plus explicit
`server.extraEnv` or `server.extraEnvFrom` entries.

### Raft

| Value | Default | Description |
| --- | --- | --- |
| `raft.groupCount` | `64` | Number of Raft groups. Renders `raft.group_count` and the gateway's `--raft-group-count`. |
| `raft.logDir` | `/var/lib/ursula/raft` | Raft log directory mounted to the `raft-data` PVC. Renders `raft.wal.path`. Must be non-empty. |
| `raft.walFsync` | `""` | When Raft WAL appends reach stable storage. Empty renders no `raft.wal.fsync`, so the server takes `always` when each group has one or two voters and `never` otherwise. `never` acknowledges once the write is in the page cache: a host crash can cost this replica its unsynced tail, and it rejoins through the recovery gate. `always` acknowledges a batch after `fsync`. Votes, group log states and the run state are always written with `fsync`. Renders `raft.wal.fsync`. |
| `raft.walSegmentBytes` | `67108864` | Size at which each core's journal starts a new segment. Purge deletes whole segments, so this is also how much disk space comes back at a time. Renders `raft.wal.segment_size`. |
| `raft.walCacheBytes` | `268435456` | Recent Raft entries kept in memory across the node, split evenly between its groups. Older entries, such as a lagging follower's catch-up, are read from disk. Renders `raft.wal.cache_size`. |
| `raft.minAvailableBytes` | `536870912` | Reject writes and readiness below this many free bytes on the WAL filesystem. `0` disables the guard. |
| `raft.resumeAvailableBytes` | `1073741824` | Free bytes required before WAL disk pressure clears; must exceed the minimum. |
| `raft.maxUncommittedBytesPerGroup` | `null` | Optional per-group cap for raft-submitted but not-yet-applied payload bytes. Renders `raft.max_uncommitted_size_per_group` in the generated config when set; `0` disables the cap. |
| `raft.snapshotPressureMaxGroupsPerTick` | `16` | Maximum groups snapshotted by one pressure pass while the node's Raft log is below its budget. Past the budget a pass starts this many for each snapshot build it keeps in flight. |
| `raft.snapshotBackstopLogs` | `null` | Far entry backstop of the snapshot driver: a group snapshots once it applied this many entries since its last snapshot, whatever its log bytes. At least 1. Renders `raft.snapshot_backstop_logs` when set. Unset, the server default of 100,000. |
| `raft.snapshotBuildMaxConcurrency` | `null` | Snapshot builds a node runs at once. Each holds a copy of its group's state and its compressed upload. The snapshot driver keeps one in flight while the node's Raft log is below its budget, and past it more, up to this many before the hard limit of twice the budget. Between 1 and 16. Renders `raft.snapshot_build_max_concurrency` when set. Unset, each server keeps its own default (2 from 0.7.4, 1 before), so a pod that still runs an older image keeps the setting it was validated with. |
| `raft.maxInSnapshotLogToKeep` | `64` | Payload-bearing Raft log entries retained per group after snapshot coverage. Renders `raft.max_in_snapshot_log_to_keep`. |
| `raft.deletedDataMaxAgeMs` | `null` | Milliseconds a replica keeps a deleted, purged or expired stream in its current Raft snapshot and its Raft log after it applies the removal. At least eight times the install snapshot timeout. Renders `raft.deleted_data_max_age` when set. Unset, each server keeps its own default, the larger of 1 hour and eight install timeouts, so a pod that still runs an older image never gets a key it does not know. |

### Persistence

Every server pod gets a `raft-data` PVC for its Raft WAL.

| Value | Default | Description |
| --- | --- | --- |
| `persistence.storageClassName` | `""` | StorageClass name. Empty uses the cluster default. |
| `persistence.accessModes` | `[ReadWriteOnce]` | PVC access modes. |
| `persistence.size` | `20Gi` | PVC size for each Ursula pod. |
| `persistence.annotations` | `{}` | Annotations applied to the PVC template. |

### S3

| Value | Default | Description |
| --- | --- | --- |
| `s3.bucket` | `""` | Shared S3 bucket. Required when `coldStorage.enabled=true` or `snapshotStore.backend=s3`. |
| `s3.region` | `""` | S3 region. Renders `storage.cold.s3.region` in the generated config when non-empty. |
| `s3.endpoint` | `""` | Optional S3-compatible endpoint, such as TOS or MinIO. |
| `s3.prefix` | `""` | Shared object prefix used with `coldStorage.prefix` to build `storage.cold.root` when cold storage is enabled. Snapshot keys also use the resolved cold root when present, so they are not always directly below `s3.prefix`. |
| `s3.credentials.accessKeyId` | `""` | Inline S3 access key rendered into a generated Secret; the entrypoint writes `storage.cold.s3.access_key_id` into the generated config. |
| `s3.credentials.secretAccessKey` | `""` | Inline S3 secret key rendered into a generated Secret; the entrypoint writes `storage.cold.s3.secret_access_key` into the generated config. |
| `s3.credentials.sessionToken` | `""` | Optional inline S3 session token rendered into a generated Secret; the entrypoint writes `storage.cold.s3.session_token` into the generated config. |
| `s3.credentials.existingSecret` | `""` | Existing Secret containing `accessKeyId`, `secretAccessKey`, and optional `sessionToken`. When set, inline credentials are not allowed. |

### Cold Storage

| Value | Default | Description |
| --- | --- | --- |
| `coldStorage.enabled` | `false` | Enable cold storage in the generated config. Multi-node production clusters should use shared object storage. |
| `coldStorage.backend` | `s3` | Cold storage backend. The chart currently renders S3 settings when enabled. |
| `coldStorage.prefix` | `cold` | Cold object prefix below `s3.prefix`. |
| `coldStorage.flush.intervalMs` | `1000` | Background cold flush interval in milliseconds. |
| `coldStorage.flush.minHotBytes` | `8388608` | Skip flush below this hot byte threshold per group. |
| `coldStorage.flush.maxBytes` | `8388608` | Maximum bytes flushed into one group-scoped cold object per tick. |
| `coldStorage.flush.maxConcurrency` | `4` | Maximum concurrent cold writes. |
| `coldStorage.flush.maxHotBytesPerGroup` | `67108864` | Hot-byte backpressure ceiling per group; `0` disables the cap. |
| `coldStorage.compaction.enabled` | `true` | Same-stream cold chunk compaction (`storage.cold.compaction_enabled`). Set `false` to turn it off. |
| `coldStorage.cache.maxSizeBytes` | `null` | Optional cold-read cache size override. Renders `storage.cold.cache.max_size`; `0` disables the cache, and omitted lets the runtime install its default cache. |
| `coldStorage.cache.blockSizeBytes` | `null` | Optional cold-read cache block size override. Renders `storage.cold.cache.block_size`. |
| `coldStorage.cache.readaheadBlocks` | `null` | Optional cold-read cache readahead override. Renders `storage.cold.cache.readahead_blocks`. |

### Snapshot Store

| Value | Default | Description |
| --- | --- | --- |
| `snapshotStore.backend` | `auto` | OpenRaft snapshot backend: `auto` (S3 with cold storage, inline otherwise), `inline` or `s3`. Rendered as `storage.snapshot.backend` in the generated config. |
| `snapshotStore.prefix` | `snapshots` | Snapshot object suffix rendered as `storage.snapshot.s3_prefix`; resolved against `storage.cold.root` when present, otherwise against the S3 bucket root for S3 snapshots. |
| `snapshotStore.driveIntervalMs` | `null` | Optional snapshot driver interval in milliseconds, at least `1`. Renders `storage.snapshot.drive_interval`. Omitted, the driver ticks every 5 seconds with external stores and every second inline. The driver always runs, so `0` is refused. |
| `snapshotStore.s3TimeoutMs` | `null` | Optional per-attempt timeout of an S3 snapshot-store request in milliseconds. Renders `storage.snapshot.s3_timeout`. Omitted keeps the server's default of 10 seconds. A PUT sends the whole object in one attempt, so this must also cover a healthy PUT of the largest snapshot. A follower's install copy, up to four requests under this timeout and `snapshotStore.s3MaxRetries`, must fit inside the install snapshot timeout, or the server refuses to start. |
| `snapshotStore.s3MaxRetries` | `null` | Optional retries of an S3 snapshot-store request. Renders `storage.snapshot.s3_max_retries`. Omitted keeps the server's default of 1. |

### Gateway

| Value | Default | Description |
| --- | --- | --- |
| `gateway.enabled` | `true` | Enable the gateway Deployment and Service. The gateway is the default external entrypoint. |
| `gateway.replicaCount` | `2` | Number of gateway replicas. |
| `gateway.image.repository` | `""` | Gateway image repository. Empty inherits `global.image.repository`. |
| `gateway.image.tag` | `""` | Gateway image tag. Empty inherits `global.image.tag` or chart `appVersion`. |
| `gateway.image.pullPolicy` | `""` | Gateway image pull policy. Empty inherits `global.image.pullPolicy`. |
| `gateway.ports.http` | `4437` | Gateway HTTP process listen port. |
| `gateway.service.type` | `ClusterIP` | Gateway Service type. |
| `gateway.service.port` | `4437` | Gateway Service port. |
| `gateway.service.trafficDistribution` | `""` | Optional Kubernetes Service traffic preference. Set `PreferSameZone` on Kubernetes 1.35+ multi-AZ clusters to keep client-to-gateway traffic zonal when a local endpoint is ready. |
| `gateway.service.annotations` | `{}` | Gateway Service annotations. |
| `gateway.ingress.enabled` | `false` | Render an Ingress resource for the gateway Service. |
| `gateway.ingress.className` | `""` | Optional IngressClass name rendered as `spec.ingressClassName`. |
| `gateway.ingress.annotations` | `{}` | Ingress annotations for controller settings such as NGINX, ALB, or cert-manager. |
| `gateway.ingress.labels` | `{}` | Extra labels applied only to the Ingress resource. |
| `gateway.ingress.hosts` | `[]` | Ingress rule hosts. Required when `gateway.ingress.enabled=true`. |
| `gateway.ingress.hosts[].host` | — | Fully-qualified host name. |
| `gateway.ingress.hosts[].paths` | `[]` | HTTP paths routed to the gateway Service named port `gateway`. |
| `gateway.ingress.hosts[].paths[].path` | — | URL path beginning with `/`. |
| `gateway.ingress.hosts[].paths[].pathType` | — | Kubernetes Ingress path type: `Prefix`, `Exact`, or `ImplementationSpecific`. |
| `gateway.ingress.tls` | `[]` | TLS entries rendered into `spec.tls`. Referenced Secrets must already exist. |
| `gateway.ingress.tls[].secretName` | — | Existing Kubernetes TLS Secret name. |
| `gateway.ingress.tls[].hosts` | `[]` | Hosts covered by the TLS Secret. |
| `gateway.upstreams` | `[]` | Manual upstream URLs. When empty, the chart auto-generates upstreams from Ursula server pod DNS through the headless Service. Each must reach one node, with nothing in between that resends requests or answers `401`, `402` or `429`. |
| `gateway.podAnnotations` | `{}` | Extra annotations applied to gateway pods. |
| `gateway.podLabels` | `{}` | Extra labels applied to gateway pods. Must not set selector labels `app.kubernetes.io/name` or `app.kubernetes.io/instance`. |
| `gateway.rustLog` | `ursula_gateway=info` | `RUST_LOG` tracing filter for the gateway. |
| `gateway.connectTimeoutSeconds` | `5` | TCP connect timeout per upstream attempt in seconds. |
| `gateway.responseHeaderTimeoutSeconds` | `5` | Timeout for upstream response headers in seconds. A read or a producer append that gets no answer within it goes on to another upstream, and any other write is answered `504`. A missing answer ejects nothing: the probes decide which upstreams answer. |
| `gateway.upstreamTcpUserTimeoutSeconds` | `5` | Seconds data sent to an upstream may stay unacknowledged before the connection closes and its request fails with `502` (Linux `TCP_USER_TIMEOUT`). Idle connections are probed three times within it. At least `1`. |
| `gateway.maxRequestBodyBytes` | `33554432` | Maximum request body bytes the gateway buffers for leader-redirect replay before returning `413 Payload Too Large`. With `gateway.bodyBudgetBytes` unset, the gateway grows its body budget to hold a body at this limit beside its 32 MiB small-body headroom. Give the pod at least twice the budget in memory. The budget cannot go below this limit plus 32 MiB, so lower this limit when the pod cannot hold that. At most `33554432`, the protocol's request limit, which every node applies: the chart refuses a larger value, and the gateway refuses to start with one. |
| `gateway.bodyBudgetBytes` | `null` | Bytes of request bodies the gateway buffers at once, across requests. Renders `--body-budget-bytes` when set, which needs a gateway of 0.7.6 or later. Unset, the gateway uses 256 MiB or `gateway.maxRequestBodyBytes` plus 32 MiB, whichever is larger. Set, it must be at least `gateway.maxRequestBodyBytes` plus 32 MiB. |
| `gateway.preStopDelaySeconds` | `10` | Seconds a terminating gateway pod keeps serving before it gets `SIGTERM`, so that every node's Service proxy and any load balancer in front stop sending it new connections before its listener closes. Raise it to the deregistration delay of a load balancer that targets pod IPs. `0` renders no preStop hook. |
| `gateway.gracefulShutdownTimeoutSeconds` | `45` | Maximum graceful drain time after `SIGTERM`, in seconds. Requests in flight finish within it, and live reads end at it. Keep it above `gateway.responseHeaderTimeoutSeconds` so that a forwarded write is answered. |
| `gateway.terminationGracePeriodSeconds` | `60` | Gateway pod grace period. Must exceed `gateway.preStopDelaySeconds` plus `gateway.gracefulShutdownTimeoutSeconds`, or the kubelet kills a drain in progress. |
| `gateway.extraArgs` | `[]` | Extra CLI args appended after generated gateway args. |
| `gateway.extraEnv` | `[]` | Extra container environment entries for the gateway. |
| `gateway.extraEnvFrom` | `[]` | Extra `envFrom` blocks for the gateway. |
| `gateway.resources` | `{}` | Container resource requests and limits for the gateway. |
| `gateway.podSecurityContext` | `{fsGroup: 10001, fsGroupChangePolicy: OnRootMismatch}` | Pod-level securityContext for gateway pods. |
| `gateway.securityContext` | `{runAsUser: 10001, runAsGroup: 10001, runAsNonRoot: true, readOnlyRootFilesystem: true, allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}` | Container-level securityContext for the gateway container. |
| `gateway.probes.startup.enabled` | `true` | Enable startupProbe for the gateway. |
| `gateway.probes.startup.failureThreshold` | `30` | Startup probe failure threshold. |
| `gateway.probes.startup.periodSeconds` | `2` | Startup probe period. |
| `gateway.probes.readiness.enabled` | `true` | Enable readinessProbe for the gateway. |
| `gateway.probes.readiness.periodSeconds` | `5` | Readiness probe period. |
| `gateway.probes.readiness.timeoutSeconds` | `2` | Readiness probe timeout. |
| `gateway.probes.liveness.enabled` | `true` | Enable livenessProbe for the gateway. |
| `gateway.probes.liveness.periodSeconds` | `10` | Liveness probe period. |
| `gateway.probes.liveness.timeoutSeconds` | `2` | Liveness probe timeout. |
| `gateway.podDisruptionBudget.enabled` | `false` | Enable a PodDisruptionBudget for the gateway. |
| `gateway.podDisruptionBudget.maxUnavailable` | `1` | Maximum voluntary disruptions for gateway replicas. |
| `gateway.autoscaling.enabled` | `false` | Enable HorizontalPodAutoscaler for the gateway. |
| `gateway.autoscaling.minReplicas` | `2` | Minimum number of gateway replicas. |
| `gateway.autoscaling.maxReplicas` | `10` | Maximum number of gateway replicas. |
| `gateway.autoscaling.targetCPUUtilizationPercentage` | `80` | Target CPU utilization percentage for HPA. |
| `gateway.autoscaling.targetMemoryUtilizationPercentage` | `null` | Target memory utilization percentage for HPA. |
| `gateway.autoscaling.behavior` | `{}` | HPA scaling behavior. |
| `gateway.scheduling.nodeSelector` | `{}` | Node selector labels for gateway pods. |
| `gateway.scheduling.tolerations` | `[]` | Tolerations for gateway pods. |
| `gateway.scheduling.affinity` | `{}` | Affinity rules for gateway pods. |
| `gateway.scheduling.topologySpreadConstraints` | `[]` | Topology spread constraints for gateway pods. |
| `gateway.initContainers` | `[]` | Init containers to run before the gateway container. |

### Helm Test

| Value | Default | Description |
| --- | --- | --- |
| `tests.enabled` | `true` | Render the Helm test Pod. |
| `tests.timeoutSeconds` | `120` | Timeout passed to `ursulactl wait-ready --timeout-secs`. |
| `tests.image.repository` | `""` | Optional test image repository. Empty inherits `global.image.repository`. |
| `tests.image.tag` | `""` | Optional test image tag. Empty inherits `global.image.tag` or chart `appVersion`. |
| `tests.image.pullPolicy` | `""` | Optional test image pull policy. Empty inherits `global.image.pullPolicy`. |
