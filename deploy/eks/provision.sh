#!/usr/bin/env bash
set -euo pipefail

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$script_dir"

command -v tofu >/dev/null 2>&1 || { echo "OpenTofu is required: https://opentofu.org/docs/intro/install/" >&2; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "AWS CLI v2 is required" >&2; exit 1; }

tofu init
tofu apply "$@"

cluster_name="$(tofu output -raw cluster_name)"
aws_region="$(tofu output -raw aws_region)"
aws eks update-kubeconfig --name "$cluster_name" --region "$aws_region"

printf '\nAWS prerequisites are ready. Continue with:\n\n'
tofu output -raw helm_install
printf '\n'
tofu output -raw helm_test
printf '\n'
