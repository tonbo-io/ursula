#!/usr/bin/env bash
set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$script_dir"

command -v tofu >/dev/null 2>&1 || { echo "OpenTofu is required: https://opentofu.org/docs/intro/install/" >&2; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "AWS CLI v2 is required" >&2; exit 1; }

test -f backend.hcl || { echo "Copy backend.hcl.example to backend.hcl and configure the production state bucket" >&2; exit 1; }
test -f terraform.tfvars || { echo "Copy terraform.tfvars.example to terraform.tfvars and set the required deployment inputs" >&2; exit 1; }
if grep -q "replace-with" backend.hcl terraform.tfvars; then
  echo "backend.hcl or terraform.tfvars still contains a replace-with placeholder" >&2
  exit 1
fi

tofu init -reconfigure -backend-config=backend.hcl
tofu apply "$@"

cluster_name="$(tofu output -raw cluster_name)"
aws_region="$(tofu output -raw aws_region)"
aws eks update-kubeconfig --name "$cluster_name" --region "$aws_region"

printf '\nAWS prerequisites are ready. Continue with:\n\n'
tofu output -raw helm_install
printf '\n'
tofu output -raw helm_test
printf '\n'
