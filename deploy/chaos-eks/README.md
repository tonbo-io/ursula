# EKS Chaos Add-on

This OpenTofu stack adds the chaos-agent Pod Identity to an existing EKS cluster and writes `generated-values.yaml` for the separate `ursula-chaos` Helm chart. It does not provision EKS and does not change the community-facing [`deploy/eks`](../eks) stack.

The target Ursula release should use three voters across three availability zones, durable Raft log PVCs, S3 cold storage and snapshots, and an enabled indexer pool. `deploy/eks` already produces that topology, but any equivalent EKS deployment can be targeted.

```bash
cp terraform.tfvars.example terraform.tfvars
tofu init
tofu apply
helm upgrade --install ursula-chaos ../../charts/ursula-chaos \
  --namespace ursula \
  -f generated-values.yaml
```

The IAM policy is scoped to one status object. Kubernetes Pod deletion is authorized separately by the chart's namespaced Role and restricted to the three configured voter Pod names.
