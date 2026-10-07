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
original_wait_for_pod_started=$(declare -f wait_for_pod_started)
# Existing resume fixtures use the same pod-identity preconditions for
# listener startup and final readiness. The ordering regression below gives
# those stages distinct behavior.
wait_for_pod_started() { wait_for_pod_ready "$1"; }
original_write_manifest=$(declare -f write_manifest)
original_record_state=$(declare -f record_state)
# Legacy state-machine fixtures below model ordering only. Instance-binding
# regressions exercise the real function in their own source-only subshell.
bind_replacement_incarnation() { :; }

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
wait_for_pod_started() { wait_for_pod_ready "$1"; }

start_forward() {
  [ "$1" = "1" ]
  resumed_forward=1
}

desired_revision() {
  printf '%s' ursula-current
}

replace_pod() {
  [ "$1" = "1" ]
  [ "$2" = "${mocked_uid}" ]
  replacement_count=$((replacement_count + 1))
  mocked_uid="replacement-${replacement_count}"
  mocked_revision=ursula-current
}

strict_verify() { :; }

record_state() {
  [ "$2" = "2" ]
  [ "$1" = complete ]
  resumed_complete=1
}

# A non-target Pod recorded as restarting started drained. It is replaced
# with the current target once, then rejoins.
replacement_count=0
resumed_forward=0
resumed_complete=0
CTL=true
resume_if_needed
[ "${replacement_count}" = "1" ]
[ "${resumed_forward}" = "1" ]
[ "${resumed_complete}" = "1" ]

# A schema-v1 state can outlive more than one failed Helm attempt. If the
# current Ready Pod has a strictly newer controller-owned sequence than the
# saved target, wait for it to catch up, undrain it and close the stale
# record without replacing it.
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
wait_for_pod_ready() { [ "$1" = "2" ]; }
wait_for_pod_started() { wait_for_pod_ready "$1"; }
start_forward() { [ "$1" = "2" ]; legacy_forward=1; }
strict_verify() { legacy_verifies=$((legacy_verifies + 1)); }
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
[ "${legacy_verifies}" = "1" ]
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

# A failed Helm attempt may have replaced the recorded source with a Pod of
# an older template that is not Ready yet. It started drained, so replace it
# with the current target before it rejoins. A state left by the removed
# restart-quiesce upgrade bridge is an unsupported phase.
(
  NAMESPACE=ursula
  STATEFULSET=ursula
  REPLICAS=3
  EXPECTED_GROUPS=256
  TARGET_IMAGE=ghcr.io/tonbo-io/ursula@sha256:new-target
  ROLLOUT_SOURCE_ONLY=1
  export NAMESPACE STATEFULSET REPLICAS EXPECTED_GROUPS TARGET_IMAGE ROLLOUT_SOURCE_ONLY
  # shellcheck source=graceful-rollout.sh
  . "${test_dir}/graceful-rollout.sh"
  bind_replacement_incarnation() { printf '%s\n' bind >>"${superseded_order}"; }

  superseded_ctl=$(mktemp)
  superseded_ctl_calls=$(mktemp)
  superseded_order=$(mktemp)
  export superseded_ctl_calls superseded_order
  cat >"${superseded_ctl}" <<'CTL'
#!/bin/sh
case "$1" in
  wait|undrain)
    printf '%s\n' "$1" >>"${superseded_ctl_calls}"
    printf '%s\n' "$1" >>"${superseded_order}"
    ;;
  *)
    printf 'unexpected superseded ursulactl invocation: %s\n' "$*" >&2
    exit 1
    ;;
esac
CTL
  chmod +x "${superseded_ctl}"
  CTL=${superseded_ctl}
  kubectl() {
    case "$*" in
      *"get configmap ursula-rollout-state -o jsonpath={.data.target-image}"*)
        printf '%s' ghcr.io/tonbo-io/ursula@sha256:old-target
        ;;
      *"get configmap ursula-rollout-state -o jsonpath={.data.target-revision}"*)
        printf '%s' ursula-old-target
        ;;
      *"get configmap ursula-rollout-state -o jsonpath={.data.state-schema-version}"*)
        printf '%s' 2
        ;;
      *"get configmap ursula-rollout-state -o jsonpath={.data.source-pod-uid}"*)
        printf '%s' drained-source-uid
        ;;
      *"get configmap ursula-rollout-state -o jsonpath={.data.phase}"*)
        printf '%s' "${superseded_phase}"
        ;;
      *"get configmap ursula-rollout-state -o jsonpath={.data.node-id}"*)
        printf '%s' 3
        ;;
      *"get configmap ursula-rollout-state"*)
        return 0
        ;;
      *"get pod ursula-2 -o jsonpath={.status.conditions"*)
        printf '%s' False
        ;;
      *"get pod ursula-2 -o jsonpath={.metadata.uid}"*)
        printf '%s' "${superseded_uid}"
        ;;
      *)
        printf 'unexpected superseded kubectl invocation: %s\n' "$*" >&2
        return 1
        ;;
    esac
  }
  desired_revision() { printf '%s' ursula-new-target; }
  wait_for_pod_ready() { [ "$1" = 2 ]; }
  wait_for_pod_started() { wait_for_pod_ready "$1"; }
  start_forward() { [ "$1" = 2 ]; superseded_forward=1; }
  strict_verify() { superseded_verified=1; }
  pod_matches_target() {
    [ "$1" = 2 ] && [ "$2" = ursula-new-target ] && [ "${superseded_revision}" = ursula-new-target ]
  }
  replace_pod() {
    [ "$1" = 2 ]
    [ "$2" = "${superseded_uid}" ]
    printf '%s\n' replace-pod >>"${superseded_order}"
    superseded_uid=current-target-uid
    superseded_revision=ursula-new-target
    superseded_replacement_count=$((superseded_replacement_count + 1))
  }
  record_state() {
    [ "$1" = complete ]
    [ "$2" = 3 ]
    superseded_complete=1
  }

  superseded_phase=upgrading-restart-quiesce
  superseded_uid=prior-target-replacement-uid
  superseded_revision=ursula-old-target
  superseded_replacement_count=0
  if resume_if_needed; then
    echo "a restart-quiesce upgrade phase must be refused" >&2
    exit 1
  fi
  [ "${superseded_replacement_count}" = 0 ]
  [ ! -s "${superseded_order}" ]

  superseded_phase=restarting
  superseded_forward=0
  superseded_verified=0
  superseded_complete=0
  resume_if_needed
  [ "${superseded_forward}" = 1 ]
  [ "${superseded_verified}" = 1 ]
  [ "${superseded_replacement_count}" = 1 ]
  [ "${superseded_complete}" = 1 ]
  [ "$(tr '\n' ' ' <"${superseded_ctl_calls}")" = "wait undrain " ]
  [ "$(tr '\n' ' ' <"${superseded_order}")" = "replace-pod bind wait undrain " ]
  rm -f "${superseded_ctl}" "${superseded_ctl_calls}" "${superseded_order}"
)

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

# A recorded replacement must be resumed before the blanket Ready gate. The
# stale replacement in the fixture cannot become Ready until resume replaces
# it, so reversing these two calls recreates the production deadlock.
call_order=
legacy_shared_store_guard() { :; }
write_manifest() { :; }
pin_manifest() { :; }
wait_for_template() { :; }
start_ready_forwards() { call_order="${call_order} ready"; }
resume_if_needed() { call_order="${call_order} resume"; }
wait_for_pod_ready() { call_order="${call_order} wait"; }
wait_for_pod_started() { wait_for_pod_ready "$1"; }
start_forward() { :; }
strict_verify() { :; }
desired_revision() { printf '%s' ursula-current; }
roll_node() { :; }
all_pods_match_target() { return 0; }
record_state() { :; }
CTL=false
REPLICAS=1
main
[ "${call_order}" = " ready resume wait" ]
eval "${original_write_manifest}"

# Every newly written state uses schema v3 and persists the source Pod UID.
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
grep -q -- '--from-literal=state-schema-version=3' "${record_state_args}"
grep -q -- '--from-literal=source-pod-uid=source-uid-2' "${record_state_args}"
rm -f "${record_state_args}" /tmp/rollout-state.yaml

# One node roll: drain, record the restart, replace the drained Pod, wait
# until the replacement is a caught-up voter, undrain, then verify.
(
  NAMESPACE=ursula
  STATEFULSET=ursula
  REPLICAS=3
  EXPECTED_GROUPS=256
  TARGET_IMAGE=ghcr.io/tonbo-io/ursula:target
  ROLLOUT_SOURCE_ONLY=1
  export NAMESPACE STATEFULSET REPLICAS EXPECTED_GROUPS TARGET_IMAGE ROLLOUT_SOURCE_ONLY
  # shellcheck source=graceful-rollout.sh
  . "${test_dir}/graceful-rollout.sh"

  roll_order=$(mktemp)
  export roll_order
  roll_ctl=$(mktemp)
  cat >"${roll_ctl}" <<'CTL'
#!/bin/sh
case "$1" in
  drain|wait|undrain)
    printf '%s\n' "$1" >>"${roll_order}"
    ;;
  *)
    printf 'unexpected roll ursulactl invocation: %s\n' "$*" >&2
    exit 1
    ;;
esac
CTL
  chmod +x "${roll_ctl}"
  CTL=${roll_ctl}
  MANIFEST=$(mktemp)
  printf '{}\n' >"${MANIFEST}"
  roll_image=ghcr.io/tonbo-io/ursula:source
  roll_revision=ursula-source
  roll_uid=source-uid
  kubectl() {
    case "$*" in
      *"get pod ursula-2 -o jsonpath={.spec.containers"*)
        printf '%s' "${roll_image}"
        ;;
      *"get pod ursula-2 -o jsonpath={.metadata.labels.controller-revision-hash}"*)
        printf '%s' "${roll_revision}"
        ;;
      *"get pod ursula-2 -o jsonpath={.metadata.uid}"*)
        printf '%s' "${roll_uid}"
        ;;
      *)
        printf 'unexpected roll kubectl invocation: %s\n' "$*" >&2
        return 1
        ;;
    esac
  }
  desired_revision() { printf '%s' ursula-target; }
  strict_verify() { printf '%s\n' verify >>"${roll_order}"; }
  record_state() { printf '%s\n' "record-$1-$2-${3:-}" >>"${roll_order}"; }
  replace_pod() {
    [ "$1" = 2 ] && [ "$2" = source-uid ]
    printf '%s\n' replace >>"${roll_order}"
    roll_image=ghcr.io/tonbo-io/ursula:target
    roll_revision=ursula-target
    roll_uid=replacement-uid
  }
  wait_for_pod_started() { [ "$1" = 2 ]; printf '%s\n' started >>"${roll_order}"; }
  start_forward() { [ "$1" = 2 ]; printf '%s\n' forward >>"${roll_order}"; }
  bind_replacement_incarnation() { [ "$1" = 3 ]; printf '%s\n' bind >>"${roll_order}"; }
  wait_for_pod_ready() { [ "$1" = 2 ]; printf '%s\n' ready >>"${roll_order}"; }

  roll_node 2
  [ "$(tr '\n' ' ' <"${roll_order}")" = "verify drain record-restarting-3-source-uid replace started forward bind wait undrain ready verify record-complete-3- " ]
  rm -f "${roll_ctl}" "${roll_order}" "${MANIFEST}"
)

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
  python3 -c 'import json,sys; nodes=json.load(open(sys.argv[1]))["nodes"]; assert all(n["metrics_url"] == n["admin_url"] and "svc.cluster.local" in n["http_url"] for n in nodes)' "${MANIFEST}"
  rm -f "${MANIFEST}"
done

# A replacement serves admin RPCs before it is a caught-up Raft voter.
# Waiting for final Ready before the catch-up wait and the undrain would
# deadlock this rollout permanently.
(
  ROLLOUT_SOURCE_ONLY=1
  . "${test_dir}/graceful-rollout.sh"
  recovery_order=
  bound=0
  caught_up=0
  released=0
  final_ready=0
  TARGET_REVISION=revision-ready-contract
  CTL=ready_contract_ctl
  wait_for_pod_started() { recovery_order="${recovery_order} started"; }
  start_forward() { recovery_order="${recovery_order} forward"; }
  pod_matches_target() { return 0; }
  bind_replacement_incarnation() { bound=1; recovery_order="${recovery_order} bound"; }
  ready_contract_ctl() {
    case "$1" in
      wait)
        [ "${bound}" = 1 ]
        caught_up=1
        recovery_order="${recovery_order} caught-up"
        ;;
      undrain)
        [ "${caught_up}" = 1 ]
        released=1
        recovery_order="${recovery_order} released"
        ;;
      *)
        return 1
        ;;
    esac
  }
  wait_for_pod_ready() {
    [ "${caught_up}" = 1 ] && [ "${released}" = 1 ]
    final_ready=1
    recovery_order="${recovery_order} ready"
  }
  strict_verify() { [ "${final_ready}" = 1 ]; recovery_order="${recovery_order} verified"; }
  record_state() { [ "$1" = complete ]; recovery_order="${recovery_order} complete"; }
  finish_restart 2 3
  [ "${recovery_order}" = " started forward bound caught-up released ready verified complete" ]
)

# Check the real startup gate independently of its fixtures above. It must
# observe the Ursula container's completed TCP startup probe, not Pod Ready.
(
  eval "${original_wait_for_pod_started}"
  NAMESPACE=ursula
  STATEFULSET=ursula
  kubectl() {
    [ "$*" = '-n ursula wait --for=jsonpath={.status.containerStatuses[?(@.name=="ursula")].started}=true pod/ursula-2 --timeout=15m' ]
  }
  wait_for_pod_started 2
)

# A stale caller cannot delete the new Pod that reused its admitted name. The
# API precondition is authoritative even when replacement races the request.
(
  . "${test_dir}/graceful-rollout.sh"
  bind_replacement_incarnation() { :; }
  NAMESPACE=ursula
  STATEFULSET=ursula
  source_uid=11111111-2222-3333-4444-555555555555
  replacement_uid=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
  identity_dir=$(mktemp -d)
  trap 'rm -rf "${identity_dir}"' EXIT
  reject_stale=false
  stop_forward() { [ "$1" = 2 ]; }
  kubectl() {
    case "$*" in
      'delete --raw=/api/v1/namespaces/ursula/pods/ursula-2 -f -')
        cat >"${identity_dir}/body.json"
        python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d == {"apiVersion":"v1", "kind":"DeleteOptions", "preconditions":{"uid":sys.argv[2]}}' \
          "${identity_dir}/body.json" "${source_uid}"
        if [ "${reject_stale}" = true ]; then
          printf 'Conflict: UID precondition does not match\n' >&2
          return 1
        fi
        touch "${identity_dir}/deleted-source"
        ;;
      '-n ursula get pod ursula-2 -o jsonpath={.metadata.uid}')
        # The replacement is already observable when the old caller wakes.
        printf '%s' "${replacement_uid}"
        ;;
      *)
        printf 'unexpected incarnation operation: %s\n' "$*" >&2
        return 1
        ;;
    esac
  }
  replace_pod 2 "${source_uid}"
  [ -f "${identity_dir}/deleted-source" ]
  rm "${identity_dir}/deleted-source"
  reject_stale=true
  if replace_pod 2 "${source_uid}"; then
    echo "stale UID conflict must stop replacement" >&2
    exit 1
  fi
  [ ! -f "${identity_dir}/deleted-source" ]
  rm "${identity_dir}/body.json"
  if replace_pod 2 'invalid"uid'; then
    echo "invalid identity must fail before any deletion" >&2
    exit 1
  fi
  [ ! -f "${identity_dir}/body.json" ]
)

echo "graceful-rollout.sh: all checks passed"

# Persist a single admitted replacement binding. Later Jobs must validate its
# saved process even if a container restarts within the same Pod UID.
(
  . "${test_dir}/graceful-rollout.sh"
  identity_dir=$(mktemp -d)
  trap 'rm -rf "${identity_dir}"' EXIT
  MANIFEST="${identity_dir}/manifest.json"
  printf '{"nodes":[]}\n' >"${MANIFEST}"
  CTL=identity_ctl
  current_uid=new-pod
  bound_uid=
  deny_survivor=false
  saved_count=0
  observation_failure=false
  kubectl() {
    case "$*" in
      *'{.data.source-pod-uid}') printf '%s' old-pod ;;
      *'{.data.state-schema-version}') [ "${observation_failure}" = false ] || return 1; printf '%s' 3 ;;
      *'{.data.replacement-pod-uid}') printf '%s' "${bound_uid}" ;;
      *'{.metadata.uid}') printf '%s' "${current_uid}" ;;
      *) return 1 ;;
    esac
  }
  identity_ctl() {
    printf '%s\n' "$*" >>"${identity_dir}/calls"
    [ "${deny_survivor}" = false ] || return 1
    printf '{"nodes":[],"pinned":true}\n'
  }
  record_state() {
    [ "$1 $2 $3 $4" = 'restarting 3 old-pod new-pod' ]
    saved_count=$((saved_count + 1))
    bound_uid=$4
  }
  current_uid=old-pod
  observation_failure=true
  if bind_replacement_incarnation 3; then echo 'failed state observation must refuse identity refresh' >&2; exit 1; fi
  [ ! -e "${identity_dir}/calls" ]
  observation_failure=false
  current_uid=old-pod
  if bind_replacement_incarnation 3; then echo 'same source Pod must not refresh identity' >&2; exit 1; fi
  [ ! -e "${identity_dir}/calls" ]
  current_uid=new-pod
  bind_replacement_incarnation 3
  [ "${saved_count}" = 1 ]
  grep -q -- '--replace-node 3' "${identity_dir}/calls"
  : >"${identity_dir}/calls"
  bind_replacement_incarnation 3
  ! grep -q -- '--replace-node' "${identity_dir}/calls"
  [ "${saved_count}" = 1 ]
  deny_survivor=true
  cp "${MANIFEST}" "${identity_dir}/before.json"
  if bind_replacement_incarnation 3; then echo 'changed saved process must refuse resume' >&2; exit 1; fi
  cmp "${MANIFEST}" "${identity_dir}/before.json"
  [ ! -e "${MANIFEST}.next" ]
  current_uid=unadmitted-second-pod
  : >"${identity_dir}/calls"
  if bind_replacement_incarnation 3; then echo 'second unadmitted Pod replacement must refuse resume' >&2; exit 1; fi
  [ ! -s "${identity_dir}/calls" ]
)

# Disabling the new value cannot reopen the legacy writer after adoption.
(
  . "${test_dir}/graceful-rollout.sh"
  guard_mode=present
  kubectl() {
    case "${guard_mode}" in
      present) printf '%s\n' configmap/ursula-maintenance ;;
      missing) return 0 ;;
      failed) return 1 ;;
    esac
  }
  if legacy_shared_store_guard; then echo 'shared store reopened legacy writer' >&2; exit 1; fi
  guard_mode=failed
  if legacy_shared_store_guard; then echo 'failed GET reopened legacy writer' >&2; exit 1; fi
  guard_mode=missing
  legacy_shared_store_guard
)
