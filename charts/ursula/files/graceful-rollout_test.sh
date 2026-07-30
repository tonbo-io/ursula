#!/usr/bin/env bash
set -eu
set -o pipefail

test_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
NAMESPACE=ursula
STATEFULSET=ursula
REPLICAS=3
EXPECTED_GROUPS=256
TARGET_IMAGE=ghcr.io/tonbo-io/ursula:0.3.12
CTL=/bin/true
ROLLOUT_SOURCE_ONLY=1
export NAMESPACE STATEFULSET REPLICAS EXPECTED_GROUPS TARGET_IMAGE CTL ROLLOUT_SOURCE_ONLY

# shellcheck source=graceful-rollout.sh
. "${test_dir}/graceful-rollout.sh"

kubectl() {
  case "$*" in
    *"get configmap ursula-rollout-state -o jsonpath={.data.target-image}"*)
      printf '%s' "${TARGET_IMAGE}"
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.phase}"*)
      printf '%s' restarting
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.node-id}"*)
      printf '%s' 2
      ;;
    *"get configmap ursula-rollout-state"*)
      return 0
      ;;
    *"get pod ursula-1 -o jsonpath={.spec.containers"*)
      printf '%s' ghcr.io/tonbo-io/ursula:0.3.11
      ;;
    *)
      printf 'unexpected kubectl invocation: %s\n' "$*" >&2
      return 1
      ;;
  esac
}

wait_for_pod_ready() {
  [ "$1" = "1" ]
}

start_forward() {
  echo "old-image resume must not open a new admin tunnel" >&2
  return 1
}

record_state() {
  [ "$1" = "pending" ]
  [ "$2" = "2" ]
  resumed_pending=1
}

resumed_pending=0
resume_if_needed
[ "${resumed_pending}" = "1" ]

mocked_revision=ursula-current
kubectl() {
  case "$*" in
    *"get pod ursula-1 -o jsonpath={.spec.containers"*)
      printf '%s' "${TARGET_IMAGE}"
      ;;
    *"get pod ursula-1 -o jsonpath={.metadata.labels.controller-revision-hash}"*)
      printf '%s' "${mocked_revision}"
      ;;
    *)
      printf 'unexpected kubectl invocation: %s\n' "$*" >&2
      return 1
      ;;
  esac
}

pod_matches_target 1 ursula-current
mocked_revision=ursula-stale
if pod_matches_target 1 ursula-current; then
  echo "same image with a stale controller revision must be rolled" >&2
  exit 1
fi

# The cluster manifest used to list three nodes literally, so any other replica
# count produced a view that disagreed with the StatefulSet it was rolling. The
# chart offers 1, 3 and 5.
for replicas in 1 3 5; do
  REPLICAS=${replicas}
  MANIFEST=$(mktemp)
  write_manifest
  ids=$(tr -d ' \n' <"${MANIFEST}" | grep -o '"id":[0-9]*' | wc -l | tr -d ' ')
  if [ "${ids}" != "${replicas}" ]; then
    echo "manifest for ${replicas} replicas listed ${ids} nodes" >&2
    exit 1
  fi
  # Node ids are one-based, ordinals zero-based, and each admin tunnel is the
  # base port plus the ordinal. Getting that pairing wrong drains the wrong node.
  last_ordinal=$((replicas - 1))
  grep -q "\"id\": ${replicas}," "${MANIFEST}"
  grep -q "127.0.0.1:$((15438 + last_ordinal))" "${MANIFEST}"
  grep -q "${STATEFULSET}-${last_ordinal}.${STATEFULSET}-headless.${NAMESPACE}.svc.cluster.local:4437" "${MANIFEST}"
  python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "${MANIFEST}"
  rm -f "${MANIFEST}"
done

echo "graceful-rollout.sh: all checks passed"
