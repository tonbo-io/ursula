# Ursula Helm Chart

This chart installs Ursula as a static-membership Raft cluster on Kubernetes. The default install starts three voter pods, 64 Raft groups, durable per-pod Raft log PVCs, a headless peer Service, an internal client/admin ClusterIP Service, a quorum-protecting PodDisruptionBudget, default multi-pod spread hints, an optional stateless gateway Deployment, and a Helm test that verifies cluster readiness with `ursulactl wait-ready`.

The chart is designed for fresh static-membership clusters. It does not perform online Raft voter expansion, voter removal, leader handoff during Kubernetes rolling updates, or dynamic voter reconfiguration. Those operations belong to the future Ursula operator workflow.

## Recommended Production Topology

- Run three Ursula voter pods across three availability zones for the normal production profile; use five voters only when tolerating two simultaneous voter failures is worth the additional write quorum cost.
- Give each voter its own zonal persistent volume for the Raft log, and configure shared S3 for cold chunks and externalized snapshots. Do not use `raft.storageMode=memory` or disable persistence in production.
- Run at least two stateless gateway replicas behind an authenticated TLS ingress or load balancer. Keep the server and peer Services private.
- Run a fixed event-time indexer worker pool. Users register JSON streams dynamically over HTTP after stream creation; adding a stream never requires a Helm upgrade.
- Keep S3 authoritative and treat indexer `emptyDir` volumes as disposable caches. Each source gets a logical S3 namespace, while workers claim record ranges across namespaces so hot streams can use multiple pods and small streams can share pods.
- Run two or more indexer workers across zones when event-time query availability or ingestion capacity matters. Claims reduce duplicate work; immutable parts and manifest CAS remain the correctness boundary.
- Give Ursula and the indexer pool separate workload identities with least-privilege access to non-overlapping S3 prefixes. Enable bucket versioning, encryption, lifecycle policy, access logging, and alerts for indexer readiness, blocked streams, source lag, task backlog, S3 errors, storage growth, and pod restarts.

This topology removes single-pod compute failures from the normal request path, but it does not make chart upgrades an automated Raft operation. Persistent Raft groups make membership initialization idempotent across restarts, but voter count must not be changed in place and server rollouts must be performed deliberately with health checks between pods.

[`examples/production-eks.yaml`](examples/production-eks.yaml) is a concrete three-AZ starting point with durable gp3 volumes, S3 cold storage and snapshots, three gateways, separate workload identities, and a two-replica indexer worker pool. For a complete AWS prerequisite flow, use [`deploy/eks`](../../deploy/eks): `tofu apply` produces `generated-values.yaml` and a dedicated kubeconfig, after which deployment is one `helm install` and one `helm test`.

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
helm install ursula oci://ghcr.io/tonbo-io/charts/ursula --version 0.3.0
```

For a local image loaded into the cluster:

```bash
helm install ursula charts/ursula \
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
  --expected-groups 64 \
  --timeout-secs 120
```

`wait-ready` succeeds only when every configured node reports the expected Raft
group count and every group has a leader.

## Access Locally

```bash
kubectl port-forward svc/ursula 4437:4437
curl http://127.0.0.1:4437/__ursula/metrics
```

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

The internal server Service and headless peer Service expose the same
`server.ports.client` process port. Ursula's current static peer URL is used
both for Raft/gRPC traffic and for leader redirects, so the chart keeps those
addresses on the client-reachable port until Ursula has separate peer and
redirect URL configuration.

## Bootstrap Behavior

`raft.initMembershipPerGroup` defaults to `true` so a fresh cluster can initialize per-group Raft membership automatically. Initialization checks the durable Raft log before changing membership, so persistent `logDir` deployments can keep the value enabled across restarts and upgrades. Memory Raft storage cannot recover a lost log and retains a separate automatic-restart guard.

## Static Membership And `server.replicaCount`

`server.replicaCount` is the initial voter count for a fresh static-membership
cluster. Supported fresh-install values are `1`, `3`, and `5`; production
clusters should use `3` or `5`.

Changing `server.replicaCount` on an initialized cluster is not safe Raft voter
reconfiguration. It can make Kubernetes pods, PVCs, and the persisted Raft
membership disagree. Safe scaling is reserved for the future operator, which
will add learners, wait for catch-up, promote voters, and remove old voters.

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

## Event-Time Indexer

The optional `ursula-indexer` runs as a fixed, cluster-level worker Deployment because it is a rebuildable projection, not part of the Raft write path. Pod count is independent of stream count. Registrations are stored in one conditionally updated S3 catalog object, and each registered source receives its own manifest, retained-record base, checkpoint, parts, claims, and error status below the pool prefix.

On EKS, prefer a separate IRSA or Pod Identity role for the indexer:

```yaml
s3:
  bucket: my-ursula-index
  region: us-east-1

indexer:
  enabled: true
  replicaCount: 2
  s3:
    prefix: indexes
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/ursula-index
```

After a user creates an `application/json` stream, register it through the internal indexer Service:

```bash
curl -X PUT http://ursula-indexer:4493/v1/indexes/browser-session-42 \
  -H 'Content-Type: application/json' \
  -d '{"stream_url":"http://ursula-gateway:4437/sessions/browser-session-42","timestamp_field":"captured_at"}'
```

Registration performs a HEAD preflight and rejects sources that are not JSON or do not advertise `json-record-coordinates-v1`. Workers split source order into claimed record ranges. Different workers may build ranges from the same hot stream concurrently; ranges may finish out of order, but `durable_through_record` advances only through the contiguous completed prefix. Claims are expiring efficiency hints rather than locks, so worker death cannot strand correctness and duplicate work converges through immutable content-addressed parts plus S3 manifest CAS.

Query one registration at `/v1/indexes/{id}/events`; inspect or resume it at `/v1/indexes/{id}/status` and `/v1/indexes/{id}/status/resume`. `GET /v1/indexes` lists registrations and `DELETE /v1/indexes/{id}` stops future scheduling without deleting authoritative index objects. Do not expose the Service directly to the internet: registration, deletion, and resume are administrative operations. Put authentication and path-aware authorization in a separate ingress or API gateway if applications need remote query access.

## Snapshot Store

The default snapshot backend is `inline`, which is also Ursula's runtime
default, so the chart renders an explicit `storage.snapshot.backend = "inline"`
entry without changing runtime semantics. To externalize Raft snapshot bytes to S3:

```yaml
snapshotStore:
  backend: s3
```

When `snapshotStore.backend=s3`, the generated config sets
`storage.snapshot.backend = "s3"` and shares the top-level `s3` bucket/credential
settings. The runtime drives one due group every 5 seconds by default. Set
`snapshotStore.driveIntervalMs=10000` to render
`storage.snapshot.drive_interval = "10000ms"`, or `0` to disable the manual driver.

It uses the shared top-level `s3` bucket/credential settings. Runtime S3 snapshot
keys are built under the resolved `storage.cold.root` plus
`storage.snapshot.s3_prefix` (rendered from `snapshotStore.prefix`). When
`coldStorage.enabled=true`, that resolved root is `s3.prefix/coldStorage.prefix`;
when cold storage is disabled while `snapshotStore.backend=s3`, the chart does not
render `storage.cold.root`, so snapshot keys are relative to the S3 bucket root
plus `snapshotStore.prefix`.

## Upgrade Limitations

Until the operator exists, Kubernetes StatefulSet rolling updates do not
transfer leaders, coordinate applied-index catch-up, or mutate Raft membership.
Use `ursulactl restart` manually for drain-aware rolling restarts when you need
operationally safe restarts on an initialized cluster.

## Values Reference

### Global

| Value | Default | Description |
| --- | --- | --- |
| `global.image.repository` | `ghcr.io/tonbo-io/ursula` | Ursula image repository. The image must contain the binaries required by the enabled chart components. |
| `global.image.tag` | `""` | Image tag. Empty uses the chart `appVersion`. |
| `global.image.pullPolicy` | `IfNotPresent` | Kubernetes image pull policy for server, gateway, and test pods. |
| `global.imagePullSecrets` | `[]` | Optional image pull secret references rendered into server, gateway, and test pods. |
| `global.clusterDomain` | `cluster.local` | Kubernetes cluster DNS domain used for generated peer FQDNs and gateway upstreams. |

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
| `server.replicaCount` | `3` | Fresh-cluster static voter pod count. Supported values are `1`, `3`, and `5`. Changing this on an initialized cluster is unsafe without the future operator workflow. |
| `server.podManagementPolicy` | `Parallel` | StatefulSet pod management policy. `Parallel` starts all static voters without serializing on per-pod readiness. |
| `server.ports.client` | `4437` | Ursula HTTP/admin process port. The client Service, headless peer Service, generated Raft peer URLs, and leader redirects target this port in the current chart. |
| `server.service.enabled` | `true` | Render the internal client/admin Service. |
| `server.service.type` | `ClusterIP` | Client/admin Service type. Allowed values are `ClusterIP`, `NodePort`, and `LoadBalancer`. |
| `server.service.port` | `4437` | Client/admin Service port. |
| `server.service.annotations` | `{}` | Client/admin Service annotations. |
| `server.headlessService.annotations` | `{}` | Headless peer Service annotations. The headless Service targets `server.ports.client` and uses `publishNotReadyAddresses: true` for stable peer DNS during bootstrap. |
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
| `server.probes.readiness` | `{enabled: true, periodSeconds: 5, timeoutSeconds: 2}` | TCP readiness probe. Rolling updates wait until the Ursula client port is actually serving before replacing the next voter. |
| `server.probes.liveness` | `{enabled: true, periodSeconds: 10, timeoutSeconds: 2}` | TCP liveness probe enabled after startup succeeds. |
| `server.podDisruptionBudget.enabled` | `true` | Render a PDB for multi-node clusters. The chart omits the PDB when `server.replicaCount=1`. |
| `server.podDisruptionBudget.maxUnavailable` | `1` | Maximum voluntary disruptions. The template fails if this value would allow loss of Raft quorum. |
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
| `raft.groupCount` | `64` | Number of Raft groups. Helm tests expect every node to report this count. |
| `raft.initMembershipPerGroup` | `true` | Idempotent per-group membership bootstrap flag; persistent groups may keep it enabled across restarts. |
| `raft.storageMode` | `logDir` | Raft storage mode: `logDir` for durable logs, `memory` for ephemeral testing. |
| `raft.logDir` | `/var/lib/ursula/raft` | Raft log directory mounted to the `raft-data` volume. |
| `raft.maxUncommittedBytesPerGroup` | `null` | Optional per-group cap for raft-submitted but not-yet-applied payload bytes. Renders `raft.max_uncommitted_size_per_group` in the generated config when set; `0` disables the cap. |
| `raft.maxInSnapshotLogToKeep` | `64` | Payload-bearing Raft log entries retained per group after snapshot coverage. Renders `raft.max_in_snapshot_log_to_keep`. |

### Persistence

| Value | Default | Description |
| --- | --- | --- |
| `persistence.enabled` | `true` | Create a per-pod `raft-data` PVC for durable Raft logs. When false, `raft-data` is an `emptyDir`. |
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
| `coldStorage.flush.maxBytes` | `8388608` | Maximum bytes flushed per group per tick. |
| `coldStorage.flush.maxConcurrency` | `4` | Maximum concurrent cold writes. |
| `coldStorage.flush.maxHotBytesPerGroup` | `67108864` | Hot-byte backpressure ceiling per group; `0` disables the cap. |
| `coldStorage.cache.maxSizeBytes` | `null` | Optional cold-read cache size override. Renders `storage.cold.cache.max_size`; `0` disables the cache, and omitted lets the runtime install its default cache. |
| `coldStorage.cache.blockSizeBytes` | `null` | Optional cold-read cache block size override. Renders `storage.cold.cache.block_size`. |
| `coldStorage.cache.readaheadBlocks` | `null` | Optional cold-read cache readahead override. Renders `storage.cold.cache.readahead_blocks`. |

### Snapshot Store

| Value | Default | Description |
| --- | --- | --- |
| `snapshotStore.backend` | `inline` | OpenRaft snapshot backend: `inline` or `s3`. Rendered as `storage.snapshot.backend` in the generated config. |
| `snapshotStore.prefix` | `snapshots` | Snapshot object suffix rendered as `storage.snapshot.s3_prefix`; resolved against `storage.cold.root` when present, otherwise against the S3 bucket root for S3 snapshots. |
| `snapshotStore.driveIntervalMs` | `null` | Optional manual snapshot driver interval in milliseconds. Renders `storage.snapshot.drive_interval`; omitted drives one due group every 5 seconds for external stores, and `0` disables the manual driver. |

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
| `gateway.upstreams` | `[]` | Manual upstream URLs. When empty, the chart auto-generates upstreams from Ursula server pod DNS through the headless Service. |
| `gateway.podAnnotations` | `{}` | Extra annotations applied to gateway pods. |
| `gateway.podLabels` | `{}` | Extra labels applied to gateway pods. Must not set selector labels `app.kubernetes.io/name` or `app.kubernetes.io/instance`. |
| `gateway.rustLog` | `ursula_gateway=info` | `RUST_LOG` tracing filter for the gateway. |
| `gateway.connectTimeoutSeconds` | `5` | TCP connect timeout per upstream attempt in seconds. |
| `gateway.responseHeaderTimeoutSeconds` | `30` | Timeout for upstream response headers in seconds. |
| `gateway.maxRequestBodyBytes` | `33554432` | Maximum request body bytes the gateway buffers for leader-redirect replay before returning `413 Payload Too Large`. |
| `gateway.gracefulShutdownTimeoutSeconds` | `3600` | Maximum graceful shutdown drain time after SIGTERM, in seconds. |
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

### Event-Time Indexer

| Value | Default | Description |
| --- | --- | --- |
| `indexer.enabled` | `false` | Deploy the dynamic event-time indexer worker pool. |
| `indexer.replicaCount` | `2` | Shared worker count; independent of registered stream count. |
| `indexer.s3.bucket` | `""` | Pool S3 bucket override; empty inherits `s3.bucket`. |
| `indexer.s3.region` | `""` | Pool S3 region override; empty inherits `s3.region`. |
| `indexer.s3.endpoint` | `""` | Pool S3-compatible endpoint override; empty inherits `s3.endpoint`. |
| `indexer.s3.prefix` | `event-index` | Authoritative catalog root containing per-stream namespaces. |
| `indexer.serviceAccount.annotations` | `{}` | Workload identity annotations such as an EKS IRSA role ARN. |
| `indexer.cache.servingMaxBytes` | `1073741824` | Shared serving-cache budget across every registration in one worker pod. |
| `indexer.cache.maintenanceMaxBytes` | `268435456` | Shared compaction/GC cache budget across every registration in one worker pod. |
| `indexer.cache.emptyDir.sizeLimit` | `2Gi` | Disposable local cache volume limit; durable index state remains in S3. |
| `indexer.ingest.flushEntries` | `65536` | Maximum buffered entries before a part flush. |
| `indexer.ingest.readBatchRecords` | `4096` | Maximum records requested in one source HTTP read. |
| `indexer.workers.concurrency` | `4` | Concurrent record-range tasks per worker pod. |
| `indexer.workers.segmentRecords` | `4096` | Maximum source records in one independently claimable work segment; effective size is capped by `readBatchRecords`. |
| `indexer.workers.leaseMs` | `60000` | Claim duration used to reduce duplicate processing; not a correctness boundary. |
| `indexer.compaction.fanIn` | `8` | Number of same-partition parts selected for bounded compaction. |
| `indexer.compaction.maxEntries` | `1000000` | Maximum entries loaded by one compaction; must cover one configured fan-in. |
| `indexer.garbageCollection.graceSeconds` | `86400` | Minimum age before unreferenced objects are eligible for deletion; values below 300 are rejected. |
| `indexer.garbageCollection.retainGenerations` | `8` | Recent manifest generations retained by GC. |
| `indexer.podDisruptionBudget.enabled` | `true` | Protect multi-worker query and ingestion availability; rejected for one replica. |
| `indexer.networkPolicy.enabled` | `true` | Limit indexer HTTP ingress to pods in the release namespace; egress stays open for DNS, source reads, and S3. |
| `indexer.resources` | production defaults | Per worker: requests 250m CPU, 512Mi memory, and 2Gi ephemeral storage; limits 2 CPU, 2Gi memory, and 3Gi ephemeral storage. |

### Helm Test

| Value | Default | Description |
| --- | --- | --- |
| `tests.enabled` | `true` | Render the Helm test Pod. |
| `tests.timeoutSeconds` | `120` | Timeout passed to `ursulactl wait-ready --timeout-secs`. |
| `tests.image.repository` | `""` | Optional test image repository. Empty inherits `global.image.repository`. |
| `tests.image.tag` | `""` | Optional test image tag. Empty inherits `global.image.tag` or chart `appVersion`. |
| `tests.image.pullPolicy` | `""` | Optional test image pull policy. Empty inherits `global.image.pullPolicy`. |
