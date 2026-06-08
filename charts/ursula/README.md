# Ursula Helm Chart

This chart installs Ursula as a static-membership Raft cluster on Kubernetes.
The default install starts three voter pods, 64 Raft groups, durable per-pod
Raft log PVCs, a headless peer Service, an internal client/admin ClusterIP
Service, a quorum-protecting PodDisruptionBudget, default multi-pod spread
hints, an optional stateless gateway Deployment, and a Helm test that verifies
cluster readiness with `ursulactl wait-ready`.

The chart is designed for fresh static-membership clusters. It does not perform
online Raft voter expansion, voter removal, leader handoff during Kubernetes
rolling updates, or post-bootstrap membership flag mutation. Those operations
belong to the future Ursula operator workflow.

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
helm install ursula oci://ghcr.io/tonbo-io/charts/ursula --version 0.1.0
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
curl http://127.0.0.1:4437/__ursula/healthz
curl http://127.0.0.1:4437/__ursula/readyz
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

The internal server Service and headless peer Service expose the `client` port
(`server.ports.client`) for stable pod DNS resolution during bootstrap and for
in-cluster access.

## Required Post-Bootstrap Step

`raft.initMembershipPerGroup` defaults to `true` so a fresh cluster can
initialize per-group Raft membership automatically. Keep it `true` only through
the first successful bootstrap.

After `helm test` passes, update your values and run:

```bash
helm upgrade ursula charts/ursula \
  --reuse-values \
  --set raft.initMembershipPerGroup=false
```

If you do not use `--reuse-values`, repeat your image overrides or use a values
file so the upgrade does not roll pods back to the chart default image.

Keep it `false` for normal restarts and upgrades. The future Ursula operator
will own this transition automatically.

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

This stores cold chunks under `s3.prefix/cold` and snapshots under
`s3.prefix/snapshots`. The chart renders inline credentials into a Secret named
`<release>-s3` and wires Ursula to it through `env.valueFrom.secretKeyRef`, so
the StatefulSet does not contain plaintext credentials.

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

When `coldStorage.enabled=true`, the chart renders only the cold-storage env
vars the Ursula process needs:

- `URSULA_COLD_BACKEND=s3`
- `URSULA_COLD_S3_BUCKET`
- non-empty optional S3/root fields such as `URSULA_COLD_S3_REGION`,
  `URSULA_COLD_S3_ENDPOINT`, and `URSULA_COLD_ROOT`
- cold flush/backpressure env vars only when the Helm value differs from the
  Ursula runtime default

## S3 With Existing Secret

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
```

By default, the chart reads `accessKeyId`, `secretAccessKey`, and
`sessionToken` from that Secret and maps them to:

- `URSULA_COLD_S3_ACCESS_KEY_ID`
- `URSULA_COLD_S3_SECRET_ACCESS_KEY`
- `URSULA_COLD_S3_SESSION_TOKEN`

Existing credential Secrets must use the default keys: `accessKeyId`,
`secretAccessKey`, and optional `sessionToken`. When
`s3.credentials.existingSecret` is set, inline credentials are not allowed.

## Snapshot Store

The default snapshot backend is `inline`, which is also Ursula's runtime
default, so the chart does not render `URSULA_SNAPSHOT_BACKEND` for the default
case. To externalize Raft snapshot bytes to S3:

```yaml
snapshotStore:
  backend: s3
```

When `snapshotStore.backend=s3`, the chart renders `URSULA_SNAPSHOT_BACKEND=s3`.
It uses the shared top-level `s3` configuration and writes snapshots below
`s3.prefix` by default. Set `snapshotStore.prefix` to append a subpath (e.g.
`snapshots`) to that root.

## Upgrade Limitations

Until the operator exists, Kubernetes StatefulSet rolling updates do not
transfer leaders, coordinate applied-index catch-up, or mutate Raft membership.
Use `ursulactl restart` manually for drain-aware rolling restarts when you need
operationally safe restarts on an initialized cluster.

## Values Reference

### Global

| Value | Default | Description |
| --- | --- | --- |
| `global.image.repository` | `ghcr.io/tonbo-io/ursula` | Ursula image repository. The image must contain `ursula`, `ursulactl`, and `ursulagw`. |
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
| `server.ports.client` | `4437` | Ursula HTTP/admin client-plane process port. Kubernetes probes and the client Service target this port. |
| `server.ports.cluster` | `4438` | Ursula cluster/Raft peer-plane process port. The headless peer Service and generated Raft peer URLs target this port. |
| `server.service.enabled` | `true` | Render the internal client/admin Service. |
| `server.service.type` | `ClusterIP` | Client/admin Service type. Allowed values are `ClusterIP`, `NodePort`, and `LoadBalancer`. |
| `server.service.port` | `4437` | Client/admin Service port. |
| `server.service.annotations` | `{}` | Client/admin Service annotations. |
| `server.headlessService.annotations` | `{}` | Headless peer Service annotations. The headless Service targets `server.ports.cluster` and uses `publishNotReadyAddresses: true` for stable peer DNS during bootstrap. |
| `server.podAnnotations` | `{}` | Extra annotations applied to Ursula server pods. |
| `server.podLabels` | `{}` | Extra labels applied to Ursula server pods. Must not set selector labels `app.kubernetes.io/name` or `app.kubernetes.io/instance`; the chart fails rendering if those keys are used. |
| `server.rustLog` | `ursula=info,ursula_runtime=info,ursula_raft=info` | `RUST_LOG` tracing filter. |
| `server.coreCount` | `4` | Ursula runtime core count. Tune this with CPU requests and limits. |
| `server.extraArgs` | `[]` | Extra CLI args appended after generated args. Use strings; with Helm CLI numeric-looking values usually require `--set-string`. |
| `server.extraEnv` | `[]` | Extra environment variables appended after chart-managed env vars. This can intentionally override generated env values; the entrypoint validates critical inputs before starting Ursula. |
| `server.extraEnvFrom` | `[]` | Extra `envFrom` blocks for server pods. |
| `server.resources` | `{}` | Container resource requests and limits for the Ursula server container. |
| `server.podSecurityContext` | `{fsGroup: 10001, fsGroupChangePolicy: OnRootMismatch}` | Pod-level securityContext for Ursula server pods. |
| `server.securityContext` | `{runAsUser: 10001, runAsGroup: 10001, runAsNonRoot: true, readOnlyRootFilesystem: true, allowPrivilegeEscalation: false, capabilities: {drop: [ALL]}}` | Container-level securityContext for the Ursula server container. |
| `server.podDisruptionBudget.enabled` | `true` | Render a PDB for multi-node clusters. The chart omits the PDB when `server.replicaCount=1`. |
| `server.podDisruptionBudget.maxUnavailable` | `1` | Maximum voluntary disruptions. The template fails if this value would allow loss of Raft quorum. |
| `server.scheduling.nodeSelector` | `{}` | Node selector labels for server pods. |
| `server.scheduling.tolerations` | `[]` | Tolerations for server pods. |
| `server.scheduling.affinity` | `{}` | Affinity rules for server pods. Empty uses the chart's default soft hostname anti-affinity when `server.replicaCount > 1`; set a non-empty value to override. |
| `server.scheduling.topologySpreadConstraints` | `[]` | Topology spread constraints for server pods. Empty uses the chart's default zone spread hint with `ScheduleAnyway` when `server.replicaCount > 1`; set a non-empty value to override. |

The StatefulSet sets `enableServiceLinks: false`, so Kubernetes does not inject
`*_SERVICE_HOST`, `*_SERVICE_PORT`, or other Service-link env vars into Ursula
pods. Apart from Kubernetes-internal variables such as `HOSTNAME`, the Ursula
container receives only chart-managed env vars plus explicit `server.extraEnv`
or `server.extraEnvFrom` entries.

### Raft

| Value | Default | Description |
| --- | --- | --- |
| `raft.groupCount` | `64` | Number of Raft groups. Helm tests expect every node to report this count. |
| `raft.initMembershipPerGroup` | `true` | One-time fresh-cluster per-group membership bootstrap flag. Set it to `false` after the first successful readiness check. |
| `raft.storageMode` | `logDir` | Raft storage mode: `logDir` for durable logs, `memory` for ephemeral testing. |
| `raft.logDir` | `/var/lib/ursula/raft` | Raft log directory mounted to the `raft-data` volume. |
| `raft.maxUncommittedBytesPerGroup` | `null` | Optional per-group cap for raft-submitted but not-yet-applied payload bytes. Renders `URSULA_RAFT_MAX_UNCOMMITTED_BYTES_PER_GROUP` when set. |

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
| `s3.region` | `""` | S3 region. Renders runtime S3 region env vars only when non-empty. |
| `s3.endpoint` | `""` | Optional S3-compatible endpoint, such as TOS or MinIO. |
| `s3.prefix` | `""` | Shared object prefix for Ursula objects. |
| `s3.credentials.accessKeyId` | `""` | Inline S3 access key rendered into a generated Secret. |
| `s3.credentials.secretAccessKey` | `""` | Inline S3 secret key rendered into a generated Secret. |
| `s3.credentials.sessionToken` | `""` | Optional inline S3 session token rendered into a generated Secret. |
| `s3.credentials.existingSecret` | `""` | Existing Secret containing `accessKeyId`, `secretAccessKey`, and optional `sessionToken`. When set, inline credentials are not allowed. |

### Cold Storage

| Value | Default | Description |
| --- | --- | --- |
| `coldStorage.enabled` | `false` | Enable S3 cold storage runtime env vars. Multi-node production clusters should use shared object storage. |
| `coldStorage.backend` | `s3` | Cold storage backend. The chart currently renders S3 settings when enabled. |
| `coldStorage.prefix` | `cold` | Cold object prefix below `s3.prefix`. |
| `coldStorage.flush.intervalMs` | `1000` | Background cold flush interval in milliseconds. Renders only when changed from Ursula's runtime default. |
| `coldStorage.flush.minHotBytes` | `8388608` | Skip flush below this hot byte threshold per group. Renders only when changed from Ursula's runtime default. |
| `coldStorage.flush.maxBytes` | `8388608` | Maximum bytes flushed per group per tick. Renders only when changed from Ursula's runtime default. |
| `coldStorage.flush.maxConcurrency` | `4` | Maximum concurrent cold writes. Renders only when changed from Ursula's runtime default. |
| `coldStorage.flush.maxHotBytesPerGroup` | `67108864` | Hot-byte backpressure ceiling per group. Renders only when changed from Ursula's runtime default. |

### Snapshot Store

| Value | Default | Description |
| --- | --- | --- |
| `snapshotStore.backend` | `inline` | OpenRaft snapshot backend: `inline` or `s3`. The default inline backend is not rendered as an env var because Ursula defaults to inline when unset. |
| `snapshotStore.prefix` | `snapshots` | Snapshot object prefix below `s3.prefix`. |

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
| `gateway.extraArgs` | `[]` | Extra CLI args appended after generated gateway args. |
| `gateway.extraEnv` | `[]` | Extra environment variables for the gateway. |
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
