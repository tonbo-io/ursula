#!/usr/bin/env bash
set -eu
set -o pipefail

test_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
NAMESPACE=ursula
STATEFULSET=ursula
REPLICAS=3
EXPECTED_GROUPS=256
TARGET_IMAGE=ghcr.io/tonbo-io/ursula:0.3.12
CTL=true
ROLLOUT_SOURCE_ONLY=1
export NAMESPACE STATEFULSET REPLICAS EXPECTED_GROUPS TARGET_IMAGE CTL ROLLOUT_SOURCE_ONLY

# shellcheck source=graceful-rollout.sh
. "${test_dir}/graceful-rollout.sh"

mocked_revision=ursula-stale
kubectl() {
  case "$*" in
    *"get configmap ursula-rollout-state -o jsonpath={.data.target-image}"*)
      printf '%s' "${TARGET_IMAGE}"
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.target-revision}"*)
      printf '%s' ursula-stale
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

wait_for_pod_ready() {
  [ "$1" = "1" ]
  [ "${replaced_stale}" = "1" ]
}

start_forward() {
  [ "$1" = "1" ]
  resumed_forward=1
}

desired_revision() {
  printf '%s' ursula-current
}

replace_pod() {
  [ "$1" = "1" ]
  replaced_stale=1
  mocked_revision=ursula-current
}

strict_verify() { :; }

record_state() {
  [ "$1" = "complete" ]
  [ "$2" = "2" ]
  resumed_complete=1
}

replaced_stale=0
resumed_forward=0
resumed_complete=0
resume_if_needed
[ "${replaced_stale}" = "1" ]
[ "${resumed_forward}" = "1" ]
[ "${resumed_complete}" = "1" ]

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

# A uniquely classified amnesiac voter is fenced, prepared through the
# authoritative ursulactl recovery command, replaced, caught up, undrained and
# strictly verified before the ordinary rollout starts.
mock_ctl=$(mktemp)
mock_ctl_calls=$(mktemp)
export mock_ctl_calls
cat >"${mock_ctl}" <<'CTL'
#!/bin/sh
case "$1" in
  classify-amnesiac)
    printf '%s\n' 3
    ;;
  prepare-amnesiac-restart|wait|undrain)
    printf '%s\n' "$1" >>"${mock_ctl_calls}"
    ;;
  *)
    printf 'unexpected ursulactl invocation: %s\n' "$*" >&2
    exit 1
    ;;
esac
CTL
chmod +x "${mock_ctl}"
CTL=${mock_ctl}
REPLICAS=3
desired_revision() { printf '%s' ursula-recovered; }
replace_pod() { [ "$1" = "2" ]; recovered_replaced=1; }
wait_for_pod_ready() { [ "$1" = "2" ]; }
pod_matches_target() { [ "$1" = "2" ] && [ "$2" = "ursula-recovered" ]; }
start_forward() { [ "$1" = "2" ]; recovered_forward=1; }
strict_verify() { recovered_verified=1; }
record_state() {
  printf '%s %s\n' "$1" "$2" >>"${mock_ctl_calls}"
}
recovered_replaced=0
recovered_forward=0
recovered_verified=0
recover_amnesiac_if_needed
[ "${recovered_replaced}" = "1" ]
[ "${recovered_forward}" = "1" ]
[ "${recovered_verified}" = "1" ]
grep -q '^prepare-amnesiac-restart$' "${mock_ctl_calls}"
grep -q '^restarting 3$' "${mock_ctl_calls}"
grep -q '^wait$' "${mock_ctl_calls}"
grep -q '^undrain$' "${mock_ctl_calls}"
grep -q '^complete 3$' "${mock_ctl_calls}"
rm -f "${mock_ctl}" "${mock_ctl_calls}"

# A recorded replacement must be resumed before the blanket Ready gate. The
# stale replacement in the fixture cannot become Ready until resume replaces
# it, so reversing these two calls recreates the production deadlock.
call_order=
write_manifest() { :; }
wait_for_template() { :; }
start_ready_forwards() { call_order="${call_order} ready"; }
resume_if_needed() { call_order="${call_order} resume"; }
wait_for_pod_ready() { call_order="${call_order} wait"; }
start_forward() { :; }
strict_verify() { :; }
desired_revision() { printf '%s' ursula-current; }
roll_node() { :; }
all_pods_match_target() { return 0; }
record_state() { :; }
healthy_ctl=$(mktemp)
cat >"${healthy_ctl}" <<'CTL'
#!/bin/sh
[ "$1" = "classify-amnesiac" ] && printf '%s\n' none
CTL
chmod +x "${healthy_ctl}"
CTL=${healthy_ctl}
REPLICAS=1
main
[ "${call_order}" = " ready resume wait" ]
rm -f "${healthy_ctl}"

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
