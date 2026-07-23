# EKS with OpenTofu

This reference stack provisions the AWS prerequisites for a production-shaped Ursula cluster and writes `generated-values.yaml` plus a dedicated `kubeconfig` for the repository Helm chart. OpenTofu owns the VPC, three private and three public subnets, one managed node group per availability zone, EKS control plane and add-ons, encrypted gp3 storage class, versioned S3 bucket, least-privilege IRSA roles for Ursula and the event-time indexer, and Pod Identity for the EBS CSI add-on.

The default topology uses three `m6i.xlarge` on-demand nodes across three availability zones and one NAT gateway per zone. It creates billable AWS resources. Review `tofu plan` and keep `s3_force_destroy=false`.

## Prerequisites

- OpenTofu 1.9 or newer
- AWS CLI v2 authenticated to the target account
- Helm 3 or newer
- kubectl

Create a versioned, encrypted S3 bucket for OpenTofu state. The state bucket is deliberately outside this stack so destroying an Ursula cluster cannot destroy its own recovery state. Copy the backend example and give every cluster a unique key:

```bash
cd deploy/eks
cp backend.tf.example backend.tf
```

The S3 backend uses OpenTofu native lockfiles, so it does not require a DynamoDB lock table. Copy the deployment inputs, set an explicit image tag, and replace the public API CIDR with the operator or CI network. World-open API CIDRs are rejected:

```bash
cp terraform.tfvars.example terraform.tfvars
```

## Deploy

The complete deployment path is four commands:

```bash
tofu init
tofu apply
KUBECONFIG=./kubeconfig helm install ursula ../../charts/ursula --namespace ursula --create-namespace -f generated-values.yaml
KUBECONFIG=./kubeconfig helm test ursula --namespace ursula
```

OpenTofu loads the ignored `backend.tf` and `terraform.tfvars` automatically. `tofu apply` writes `generated-values.yaml` and a cluster-specific `kubeconfig`; it never mutates `~/.kube/config`. The kubeconfig uses `aws eks get-token`, so AWS CLI remains a runtime prerequisite for Helm authentication. The generated values use fixed ServiceAccount names annotated with their IRSA role ARNs, enable S3 cold storage and snapshots, create the configured gateway and dynamic indexer worker counts, and use the generated gp3 StorageClass for voter PVCs.

OpenTofu exposes the small set of capacity decisions that affect AWS or the production baseline: node instance types and counts, voter cores and memory, Raft group count and PVC size, and gateway/indexer replicas. Keep workload-specific or experimental tuning in a second Helm values file passed after `generated-values.yaml`.

For a published chart, replace `../../charts/ursula` with `oci://ghcr.io/tonbo-io/charts/ursula --version 0.3.2`.

## Destroy

Uninstall Ursula before destroying the prerequisites so Kubernetes can detach its EBS volumes:

```bash
helm uninstall ursula --namespace ursula
tofu destroy
```

The S3 bucket is protected from deletion while it contains objects. Archive or explicitly remove the Ursula prefixes before destroying a disposable environment; do not set `s3_force_destroy=true` for production data.

Versioning retains overwritten and application-deleted objects for 30 days by default. A disposable high-churn chaos environment can set `s3_noncurrent_version_expiration_days=1`; production deployments should keep a recovery window appropriate for their restore policy.
