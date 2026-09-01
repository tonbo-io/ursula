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
original_prepare_recovery_restart=$(declare -f prepare_recovery_restart)
original_record_state=$(declare -f record_state)

mocked_revision=ursula-stale
mocked_uid=legacy-partial-uid
kubectl() {
  case "$*" in
    *"get configmap ursula-rollout-state -o jsonpath={.data.target-image}"*)
      printf '%s' "${TARGET_IMAGE}"
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.target-revision}"*)
      printf '%s' ursula-stale
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.state-schema-version}"*|\
    *"get configmap ursula-rollout-state -o jsonpath={.data.source-pod-uid}"*)
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
    *"get pod ursula-1 -o jsonpath={.status.conditions"*)
      printf '%s' False
      ;;
    *"get pod ursula-1 -o jsonpath={.metadata.uid}"*)
      printf '%s' "${mocked_uid}"
      ;;
    *)
      printf 'unexpected kubectl invocation: %s\n' "$*" >&2
      return 1
      ;;
  esac
}

wait_for_pod_ready() {
  [ "$1" = "1" ]
  [ "${replacement_count}" -ge 1 ]
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
  replacement_count=$((replacement_count + 1))
  mocked_uid="replacement-${replacement_count}"
  mocked_revision=ursula-current
}

strict_verify() { :; }

prepare_recovery_restart() {
  [ "$1" = "2" ]
  resumed_prepare_recovery=1
}

prepare_recovery_handoff() {
  [ "$1" = "2" ]
  resumed_prepare_handoff=1
}

record_state() {
  [ "$2" = "2" ]
  case "$1" in
    upgrading-restart-quiesce)
      [ "$3" = legacy-partial-uid ]
      resumed_upgrade_state=1
      ;;
    restarting)
      [ "$3" = replacement-1 ]
      resumed_restarting_state=1
      ;;
    complete)
      resumed_complete=1
      ;;
    *)
      return 1
      ;;
  esac
}

replacement_count=0
resumed_forward=0
resumed_complete=0
resumed_prepare_recovery=0
resumed_prepare_handoff=0
resumed_upgrade_state=0
resumed_restarting_state=0
CTL=true
resume_if_needed
[ "${replacement_count}" = "2" ]
[ "${resumed_forward}" = "1" ]
[ "${resumed_prepare_handoff}" = "1" ]
[ "${resumed_prepare_recovery}" = "1" ]
[ "${resumed_upgrade_state}" = "1" ]
[ "${resumed_restarting_state}" = "1" ]
[ "${resumed_complete}" = "1" ]

# A schema-v1 state can outlive more than one failed Helm attempt. If the
# current Ready Pod has a strictly newer controller-owned sequence than the
# saved target, verify it as a complete voter and close the stale record. Do
# not call the restart-quiesce endpoint: the concrete legacy Pod may predate it.
legacy_ctl=$(mktemp)
legacy_ctl_calls=$(mktemp)
export legacy_ctl_calls
cat >"${legacy_ctl}" <<'CTL'
#!/bin/sh
case "$1" in
  wait|undrain)
    printf '%s\n' "$1" >>"${legacy_ctl_calls}"
    ;;
  *)
    printf 'unexpected legacy ursulactl invocation: %s\n' "$*" >&2
    exit 1
    ;;
esac
CTL
chmod +x "${legacy_ctl}"
CTL=${legacy_ctl}
kubectl() {
  case "$*" in
    *"get configmap ursula-rollout-state -o jsonpath={.data.target-image}"*)
      printf '%s' ghcr.io/tonbo-io/ursula@sha256:saved
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.target-revision}"*)
      printf '%s' ursula-revision-13
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.state-schema-version}"*)
      printf '%s' "${legacy_state_schema}"
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.source-pod-uid}"*)
      printf '%s' "${legacy_source_pod_uid}"
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.phase}"*)
      printf '%s' restarting
      ;;
    *"get configmap ursula-rollout-state -o jsonpath={.data.node-id}"*)
      printf '%s' 3
      ;;
    *"get configmap ursula-rollout-state"*)
      return 0
      ;;
    *"get pod ursula-2 -o jsonpath={.status.conditions"*)
      printf '%s' True
      ;;
    *"get pod ursula-2 -o jsonpath={.metadata.uid}"*)
      printf '%s' newer-pod-uid
      ;;
    *"get pod ursula-2 -o jsonpath={.metadata.labels.controller-revision-hash}"*)
      printf '%s' ursula-revision-14
      ;;
    *"get controllerrevision ursula-revision-13 -o jsonpath={.revision}"*)
      printf '%s' 13
      ;;
    *"get controllerrevision ursula-revision-14 -o jsonpath={.revision}"*)
      printf '%s' "${legacy_current_sequence}"
      ;;
    *)
      printf 'unexpected legacy kubectl invocation: %s\n' "$*" >&2
      return 1
      ;;
  esac
}
desired_revision() { printf '%s' ursula-revision-15; }
start_forward() { [ "$1" = "2" ]; legacy_forward=1; }
strict_verify() { legacy_verifies=$((legacy_verifies + 1)); }
prepare_recovery_restart() { legacy_destructive_call=1; return 1; }
replace_pod() { legacy_destructive_call=1; return 1; }
record_state() {
  [ "$1" = complete ]
  [ "$2" = 3 ]
  legacy_complete=1
}
legacy_forward=0
legacy_verifies=0
legacy_destructive_call=0
legacy_complete=0
legacy_current_sequence=14
legacy_state_schema=
legacy_source_pod_uid=
resume_if_needed
[ "${legacy_forward}" = "1" ]
[ "${legacy_verifies}" = "2" ]
[ "${legacy_destructive_call}" = "0" ]
[ "${legacy_complete}" = "1" ]
[ "$(tr '\n' ' ' <"${legacy_ctl_calls}")" = "wait undrain " ]
legacy_current_sequence=12
if replacement_attempt_was_superseded 2 ursula-revision-13 ''; then
  echo "an older ControllerRevision must not supersede saved rollout state" >&2
  exit 1
fi
legacy_state_schema=2
if resume_if_needed; then
  echo "schema v2 restarting state without its source Pod UID must fail closed" >&2
  exit 1
fi
rm -f "${legacy_ctl}" "${legacy_ctl_calls}"

# Schema v2 uses the source Pod UID and therefore does not need legacy
# ControllerRevision access. An unchanged UID is not proof of replacement.
kubectl() {
  case "$*" in
    *"get pod ursula-0 -o jsonpath={.status.conditions"*)
      printf '%s' True
      ;;
    *"get pod ursula-0 -o jsonpath={.metadata.uid}"*)
      printf '%s' "${current_uid}"
      ;;
    *"get controllerrevision"*)
      controller_revision_read=1
      return 1
      ;;
    *)
      printf 'unexpected UID kubectl invocation: %s\n' "$*" >&2
      return 1
      ;;
  esac
}
controller_revision_read=0
current_uid=new-uid
replacement_attempt_was_superseded 0 ignored old-uid
[ "${controller_revision_read}" = "0" ]
current_uid=old-uid
if replacement_attempt_was_superseded 0 ignored old-uid; then
  echo "an unchanged source Pod UID must not close restarting state" >&2
  exit 1
fi
[ "${controller_revision_read}" = "0" ]

eval "${original_prepare_recovery_restart}"
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
    *"get pod ursula-2 -o jsonpath={.metadata.uid}"*)
      printf '%s' amnesiac-source-uid
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

eval "${original_prepare_recovery_restart}"

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

# Every newly written state uses schema v2 and persists the source Pod UID.
eval "${original_record_state}"
record_state_args=$(mktemp)
kubectl() {
  printf '%s\n' "$*" >>"${record_state_args}"
  case "$*" in
    *"create configmap"*)
      printf '%s\n' 'apiVersion: v1' 'kind: ConfigMap'
      ;;
  esac
}
export TARGET_REVISION=ursula-current
record_state restarting 2 source-uid-2
grep -q -- '--from-literal=state-schema-version=2' "${record_state_args}"
grep -q -- '--from-literal=source-pod-uid=source-uid-2' "${record_state_args}"
rm -f "${record_state_args}" /tmp/rollout-state.yaml

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
