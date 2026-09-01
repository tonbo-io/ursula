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
original_write_manifest=$(declare -f write_manifest)
original_reassert_restart_fence=$(declare -f reassert_restart_fence)
original_finish_rearmed_restart=$(declare -f finish_rearmed_restart)

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

reassert_restart_fence() {
  [ "$1" = "2" ]
  resumed_reassert=1
  REASSERT_RESULT=restart-required
}

finish_rearmed_restart() {
  [ "$1" = "2" ]
  [ "$2" = "1" ]
  [ "${REASSERT_RESULT}" = "restart-required" ]
  resumed_finish=1
  REASSERT_RESULT=ready
}

record_state() {
  [ "$1" = "complete" ]
  [ "$2" = "2" ]
  resumed_complete=1
}

replaced_stale=0
resumed_forward=0
resumed_complete=0
resumed_reassert=0
resumed_finish=0
CTL=true
resume_if_needed
[ "${replaced_stale}" = "1" ]
[ "${resumed_forward}" = "1" ]
[ "${resumed_reassert}" = "1" ]
[ "${resumed_finish}" = "1" ]
[ "${resumed_complete}" = "1" ]
eval "${original_reassert_restart_fence}"
eval "${original_finish_rearmed_restart}"
CTL=true

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
  reassert-restart-fence)
    printf '%s\n' "$1" >>"${mock_ctl_calls}"
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--result-file" ]; then
        printf '%s\n' ready >"$2"
        break
      fi
      shift
    done
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
grep -q '^reassert-restart-fence$' "${mock_ctl_calls}"
grep -q '^restarting 3$' "${mock_ctl_calls}"
grep -q '^wait$' "${mock_ctl_calls}"
grep -q '^undrain$' "${mock_ctl_calls}"
grep -q '^complete 3$' "${mock_ctl_calls}"
rm -f "${mock_ctl}" "${mock_ctl_calls}"

# Re-arming a live replacement is not enough: prepare every group before the
# next memory-WAL restart, then reassert the new process-local fence.
rearmed_calls=$(mktemp)
rearmed_ctl=$(mktemp)
export rearmed_calls
cat >"${rearmed_ctl}" <<'CTL'
#!/bin/sh
case "$1" in
  prepare-restart|wait)
    printf '%s\n' "$1" >>"${rearmed_calls}"
    ;;
  *)
    exit 1
    ;;
esac
CTL
chmod +x "${rearmed_ctl}"
CTL=${rearmed_ctl}
TARGET_REVISION=ursula-recovered
REASSERT_RESULT=restart-required
record_state() { printf '%s %s\n' "$1" "$2" >>"${rearmed_calls}"; }
replace_pod() { [ "$1" = "2" ]; printf '%s\n' replace >>"${rearmed_calls}"; }
wait_for_pod_ready() { [ "$1" = "2" ]; printf '%s\n' ready >>"${rearmed_calls}"; }
pod_matches_target() { [ "$1" = "2" ] && [ "$2" = "ursula-recovered" ]; }
start_forward() { [ "$1" = "2" ]; printf '%s\n' forward >>"${rearmed_calls}"; }
reassert_restart_fence() {
  [ "$1" = "3" ]
  printf '%s\n' reassert >>"${rearmed_calls}"
  REASSERT_RESULT=ready
}
finish_rearmed_restart 3 2
cat >"${rearmed_calls}.expected" <<'CALLS'
prepare-restart
restarting 3
replace
ready
forward
wait
reassert
CALLS
cmp "${rearmed_calls}.expected" "${rearmed_calls}"
rm -f "${rearmed_ctl}" "${rearmed_calls}" "${rearmed_calls}.expected"
eval "${original_reassert_restart_fence}"
eval "${original_finish_rearmed_restart}"

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
eval "${original_write_manifest}"

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
