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
kubectl wait --namespace "${namespace}" --for=create "pod/${release}-0" --timeout=30s >/dev/null
source_uid="$(kubectl get pod "${release}-0" --namespace "${namespace}" -o jsonpath='{.metadata.uid}')"
source_image="$(kubectl get pod "${release}-0" --namespace "${namespace}" -o jsonpath='{.spec.containers[0].image}')"

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

# The migration hook is idempotent. A second upgrade stages a distinct image
# without replacing the existing unscheduled Pod: OnDelete does not run an
# automatic voter rollout or require the removed maintenance shell hooks.
# shellcheck disable=SC2086
helm upgrade "${release}" "${chart}" \
  --namespace "${namespace}" \
  ${common_values} \
  --set server.updateStrategy=OnDelete \
  --set global.image.tag=ondelete-staged-test \
  --timeout 3m >/dev/null

test "$(kubectl get statefulset "${release}" --namespace "${namespace}" -o jsonpath='{.spec.updateStrategy.type}')" = "OnDelete"
test -z "$(kubectl get statefulset "${release}" --namespace "${namespace}" -o jsonpath='{.spec.updateStrategy.rollingUpdate}')"
test "$(kubectl get pod "${release}-0" --namespace "${namespace}" -o jsonpath='{.metadata.uid}')" = "${source_uid}"
test "$(kubectl get pod "${release}-0" --namespace "${namespace}" -o jsonpath='{.spec.containers[0].image}')" = "${source_image}"
test -z "$(kubectl get pod "${release}-0" --namespace "${namespace}" -o jsonpath='{.metadata.deletionTimestamp}')"
staged_image="$(kubectl get statefulset "${release}" --namespace "${namespace}" -o jsonpath='{.spec.template.spec.containers[0].image}')"
test "${staged_image}" != "${source_image}"

echo "Helm cleared stale rollingUpdate state and staged repeated OnDelete upgrades without replacing the Pod"
