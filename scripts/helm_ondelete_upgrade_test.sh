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

# Real API regression: the stale source UID must not delete a second Pod that
# the StatefulSet created under the same name. This Pod is unscheduled, so the
# test does not start a process or bypass its configured termination grace.
(
  NAMESPACE=${namespace}
  STATEFULSET=${release}
  REPLICAS=1
  EXPECTED_GROUPS=1
  TARGET_IMAGE=unused
  ROLLOUT_SOURCE_ONLY=1
  export NAMESPACE STATEFULSET REPLICAS EXPECTED_GROUPS TARGET_IMAGE ROLLOUT_SOURCE_ONLY
  . "${chart}/files/graceful-rollout.sh"
  source_uid=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-0" -o jsonpath='{.metadata.uid}')
  replace_pod 0 "${source_uid}"
  replacement_uid=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-0" -o jsonpath='{.metadata.uid}')
  test "${source_uid}" != "${replacement_uid}"
  if replace_pod 0 "${source_uid}"; then
    echo "stale source UID deleted a replacement Pod" >&2
    exit 1
  fi
  test "$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-0" -o jsonpath='{.metadata.uid}')" = "${replacement_uid}"
  test -z "$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-0" -o jsonpath='{.metadata.deletionTimestamp}')"
)

echo "Helm cleared stale rollingUpdate state, switched to OnDelete, and repeated cleanly"
