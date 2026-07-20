# EKS with OpenTofu

This reference stack provisions the AWS prerequisites for a production-shaped Ursula cluster and writes `generated-values.yaml` for the repository Helm chart. OpenTofu owns the VPC, three private and three public subnets, one managed node group per availability zone, EKS control plane and add-ons, encrypted gp3 storage class, versioned S3 bucket, least-privilege Pod Identity roles, and the Pod Identity associations for Ursula and the event-time indexer.

The default topology uses three `m6i.xlarge` on-demand nodes across three availability zones and one NAT gateway per zone. It creates billable AWS resources. Review `tofu plan`, use a remote state backend for a shared production environment, and keep `s3_force_destroy=false`.

## Prerequisites

- OpenTofu 1.9 or newer
- AWS CLI v2 authenticated to the target account
- Helm 3 or newer
- kubectl

Copy the example inputs and replace the public API CIDR with the operator or CI network:

```bash
cd deploy/eks
cp terraform.tfvars.example terraform.tfvars
```

## Deploy

The complete happy path is three commands:

```bash
./provision.sh
helm install ursula ../../charts/ursula --namespace ursula --create-namespace -f generated-values.yaml
helm test ursula --namespace ursula
```

`provision.sh` runs `tofu init`, applies the stack, writes `generated-values.yaml`, and updates the current kubeconfig through `aws eks update-kubeconfig`. The generated values use fixed ServiceAccount names that match the EKS Pod Identity associations, enable S3 cold storage and snapshots, create three gateways and two dynamic indexer workers, and use the generated gp3 StorageClass for voter PVCs.

For a published chart, replace `../../charts/ursula` with `oci://ghcr.io/tonbo-io/charts/ursula --version 0.2.0`.

## Destroy

Uninstall Ursula before destroying the prerequisites so Kubernetes can detach its EBS volumes:

```bash
helm uninstall ursula --namespace ursula
tofu destroy
```

The S3 bucket is protected from deletion while it contains objects. Archive or explicitly remove the Ursula prefixes before destroying a disposable environment; do not set `s3_force_destroy=true` for production data.
