#!/bin/sh
set -eu

namespace="ursula-helm-upgrade-${$}"
release="probe"
chart="charts/ursula"

cleanup() {
  helm uninstall "${release}" --namespace "${namespace}" >/dev/null 2>&1 || true
  kubectl delete namespace "${namespace}" --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

kubectl create namespace "${namespace}" >/dev/null

common_values="
  --set fullnameOverride=${release}
  --set server.replicaCount=1
  --set raft.storageMode=memory
  --set persistence.enabled=false
  --set s3.bucket=unused
  --set gateway.enabled=false
  --set server.scheduling.nodeSelector.ursula-test=never
"

# shellcheck disable=SC2086
helm install "${release}" "${chart}" \
  --namespace "${namespace}" \
  ${common_values} >/dev/null

kubectl patch statefulset "${release}" \
  --namespace "${namespace}" \
  --type=merge \
  --patch='{"spec":{"updateStrategy":{"type":"RollingUpdate","rollingUpdate":{"partition":2}}}}' \
  >/dev/null

before="$(kubectl get statefulset "${release}" \
  --namespace "${namespace}" \
  -o jsonpath='{.spec.updateStrategy.rollingUpdate.partition}')"
test "${before}" = "2"

# This is the regression path from #180: Helm must execute the pre-upgrade
# migration before its typed StatefulSet apply changes the strategy.
# shellcheck disable=SC2086
helm upgrade "${release}" "${chart}" \
  --namespace "${namespace}" \
  ${common_values} \
  --set server.updateStrategy=OnDelete \
  --timeout 3m >/dev/null

strategy="$(kubectl get statefulset "${release}" \
  --namespace "${namespace}" \
  -o jsonpath='{.spec.updateStrategy.type}')"
rolling="$(kubectl get statefulset "${release}" \
  --namespace "${namespace}" \
  -o jsonpath='{.spec.updateStrategy.rollingUpdate}')"

test "${strategy}" = "OnDelete"
test -z "${rolling}"

# The hook also runs on later OnDelete upgrades and must remain idempotent.
# shellcheck disable=SC2086
helm upgrade "${release}" "${chart}" \
  --namespace "${namespace}" \
  ${common_values} \
  --set server.updateStrategy=OnDelete \
  --timeout 3m >/dev/null

echo "Helm cleared stale rollingUpdate state, switched to OnDelete, and repeated cleanly"
