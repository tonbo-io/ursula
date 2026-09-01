#!/bin/sh
set -eu

: "${NAMESPACE:?}"
: "${STATEFULSET:?}"
: "${REPLICAS:?}"
: "${EXPECTED_GROUPS:?}"
: "${TARGET_IMAGE:?}"

# Ports the chart binds. Defaulted so the script stays runnable by hand against
# a cluster installed with chart defaults.
CLIENT_PORT=${CLIENT_PORT:-4437}
ADMIN_PORT=${ADMIN_PORT:-4438}
# Local end of each port-forward. Node ids are one-based and pod ordinals are
# zero-based, so node N listens on BASE + N - 1.
FORWARD_PORT_BASE=${FORWARD_PORT_BASE:-15438}

CTL=${CTL:-/tools/ursulactl}
STATE_CONFIGMAP="${STATEFULSET}-rollout-state"
MANIFEST=/tmp/cluster.json
TARGET_REVISION=
PREPARED_RESTART_NODE=

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

write_manifest() {
  {
    printf '{\n  "nodes": [\n'
    ordinal=0
    while [ "${ordinal}" -lt "${REPLICAS}" ]; do
      if [ "${ordinal}" -gt 0 ]; then
        printf ',\n'
      fi
      printf '    {\n'
      printf '      "id": %s,\n' "$((ordinal + 1))"
      printf '      "admin_url": "http://127.0.0.1:%s",\n' "$((FORWARD_PORT_BASE + ordinal))"
      printf '      "host": "%s-%s",\n' "${STATEFULSET}" "${ordinal}"
      printf '      "http_url": "http://%s-%s.%s-headless.%s.svc.cluster.local:%s"\n' \
        "${STATEFULSET}" "${ordinal}" "${STATEFULSET}" "${NAMESPACE}" "${CLIENT_PORT}"
      printf '    }'
      ordinal=$((ordinal + 1))
    done
    printf '\n  ]\n}\n'
  } >"${MANIFEST}"
}

forward_pid() {
  eval "printf '%s' \"\${PF_$1_PID:-}\""
}

stop_forward() {
  ordinal=$1
  pid=$(forward_pid "${ordinal}")
  if [ -n "${pid}" ]; then
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
    eval "PF_${ordinal}_PID=''"
  fi
}

start_forward() {
  ordinal=$1
  stop_forward "${ordinal}"
  local_port=$((FORWARD_PORT_BASE + ordinal))
  pod="${STATEFULSET}-${ordinal}"
  kubectl -n "${NAMESPACE}" port-forward "pod/${pod}" "${local_port}:${ADMIN_PORT}" \
    >"/tmp/port-forward-${ordinal}.log" 2>&1 &
  pid=$!
  eval "PF_${ordinal}_PID=${pid}"
  attempts=0
  while :; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      cat "/tmp/port-forward-${ordinal}.log" >&2
      return 1
    fi
    if grep -q 'Forwarding from' "/tmp/port-forward-${ordinal}.log"; then
      return 0
    fi
    attempts=$((attempts + 1))
    if [ "${attempts}" -ge 60 ]; then
      cat "/tmp/port-forward-${ordinal}.log" >&2
      return 1
    fi
    sleep 1
  done
}

stop_forwards() {
  ordinal=0
  while [ "${ordinal}" -lt "${REPLICAS}" ]; do
    stop_forward "${ordinal}"
    ordinal=$((ordinal + 1))
  done
}

abort_incomplete_prepared_restart() {
  if [ -z "${PREPARED_RESTART_NODE}" ] || [ ! -f "${MANIFEST}" ]; then
    return 0
  fi
  log "releasing survivor fences for failed prepared restart at node ${PREPARED_RESTART_NODE}"
  "${CTL}" abort-prepared-restart \
    --config "${MANIFEST}" \
    --node "${PREPARED_RESTART_NODE}" \
    --http-timeout-secs 60 || true
  PREPARED_RESTART_NODE=
}

cleanup() {
  abort_incomplete_prepared_restart
  stop_forwards
}
trap cleanup EXIT INT TERM

finish_prepared_restart() {
  node_id=$1
  "${CTL}" finish-prepared-restart \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --http-timeout-secs 60
  PREPARED_RESTART_NODE=
}

repair_restarted_voter() {
  node_id=$1
  PREPARED_RESTART_NODE=${node_id}
  # The first replacement can be repaired through 0.4.8 survivors, whose
  # learner endpoint ignores blocking=false and waits for catch-up. The CLI
  # bounds each ambiguous attempt and verifies the Raft postcondition before
  # retrying, so the request budget no longer becomes the rollout stall bound.
  "${CTL}" repair-restarted-voter \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --drain-timeout-secs 300 \
    --http-timeout-secs 60 \
    --lag-tolerance 16
}

wait_for_template() {
  attempts=0
  while :; do
    image=$(kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" \
      -o jsonpath='{.spec.template.spec.containers[?(@.name=="ursula")].image}')
    strategy=$(kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" \
      -o jsonpath='{.spec.updateStrategy.type}')
    replicas=$(kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" \
      -o jsonpath='{.spec.replicas}')
    generation=$(kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" \
      -o jsonpath='{.metadata.generation}')
    observed_generation=$(kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" \
      -o jsonpath='{.status.observedGeneration}')
    revision=$(kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" \
      -o jsonpath='{.status.updateRevision}')
    if [ "${image}" = "${TARGET_IMAGE}" ] &&
       [ "${strategy}" = "OnDelete" ] &&
       [ "${replicas}" = "${REPLICAS}" ] &&
       [ "${observed_generation}" = "${generation}" ] &&
       [ -n "${revision}" ]; then
      TARGET_REVISION=${revision}
      log "target template staged: image=${TARGET_IMAGE} revision=${TARGET_REVISION}"
      return 0
    fi
    attempts=$((attempts + 1))
    if [ "${attempts}" -ge 120 ]; then
      log "target template not staged: image=${image} strategy=${strategy} replicas=${replicas} generation=${generation} observed=${observed_generation} revision=${revision}"
      return 1
    fi
    sleep 1
  done
}

desired_revision() {
  kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" \
    -o jsonpath='{.status.updateRevision}'
}

pod_matches_target() {
  ordinal=$1
  expected_revision=$2
  pod="${STATEFULSET}-${ordinal}"
  image=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.spec.containers[?(@.name=="ursula")].image}')
  revision=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.labels.controller-revision-hash}')
  [ "${image}" = "${TARGET_IMAGE}" ] && [ "${revision}" = "${expected_revision}" ]
}

all_pods_match_target() {
  expected_revision=$1
  ordinal=0
  while [ "${ordinal}" -lt "${REPLICAS}" ]; do
    if ! pod_matches_target "${ordinal}" "${expected_revision}"; then
      return 1
    fi
    ordinal=$((ordinal + 1))
  done
}

wait_for_pod_ready() {
  ordinal=$1
  pod="${STATEFULSET}-${ordinal}"
  kubectl -n "${NAMESPACE}" wait --for=condition=Ready "pod/${pod}" --timeout=15m
}

start_ready_forwards() {
  ordinal=0
  while [ "${ordinal}" -lt "${REPLICAS}" ]; do
    pod="${STATEFULSET}-${ordinal}"
    ready=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
    if [ "${ready}" = "True" ]; then
      start_forward "${ordinal}"
    fi
    ordinal=$((ordinal + 1))
  done
}

replace_pod() {
  ordinal=$1
  pod="${STATEFULSET}-${ordinal}"
  old_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  stop_forward "${ordinal}"
  if [ -n "${old_uid}" ]; then
    kubectl -n "${NAMESPACE}" delete pod "${pod}" --wait=false
  fi
  attempts=0
  while :; do
    new_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
      -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    if [ -n "${new_uid}" ] && [ "${new_uid}" != "${old_uid}" ]; then
      return 0
    fi
    attempts=$((attempts + 1))
    [ "${attempts}" -lt 300 ]
    sleep 1
  done
}

strict_verify() {
  "${CTL}" wait-ready \
    --config "${MANIFEST}" \
    --expected-groups "${EXPECTED_GROUPS}" \
    --timeout-secs 300 \
    --poll-interval-secs 2
  "${CTL}" verify-cluster \
    --config "${MANIFEST}" \
    --timeout-secs 300 \
    --poll-interval-secs 2 \
    --lag-tolerance 16
}

prepare_recovery_handoff() {
  node_id=$1
  "${CTL}" prepare-recovery-handoff \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --drain-timeout-secs 300 \
    --http-timeout-secs 60 \
    --lag-tolerance 16
}

restart_quiesce_capability() {
  node_id=$1
  if ! capability=$("${CTL}" restart-quiesce-capability \
      --config "${MANIFEST}" \
      --node "${node_id}" \
      --http-timeout-secs 60); then
    return 1
  fi
  case "${capability}" in
    supported|legacy-unavailable)
      printf '%s' "${capability}"
      ;;
    *)
      log "invalid restart-quiesce capability for node ${node_id}: ${capability}" >&2
      return 1
      ;;
  esac
}

record_state() {
  phase=$1
  node_id=$2
  source_pod_uid=${3:-}
  state_file=/tmp/rollout-state.yaml
  kubectl -n "${NAMESPACE}" create configmap "${STATE_CONFIGMAP}" \
    --from-literal=state-schema-version="2" \
    --from-literal=target-image="${TARGET_IMAGE}" \
    --from-literal=target-revision="${TARGET_REVISION}" \
    --from-literal=phase="${phase}" \
    --from-literal=node-id="${node_id}" \
    --from-literal=source-pod-uid="${source_pod_uid}" \
    --dry-run=client -o yaml >"${state_file}"
  kubectl -n "${NAMESPACE}" apply -f "${state_file}"
}

replacement_attempt_was_superseded() {
  ordinal=$1
  saved_revision=$2
  source_pod_uid=$3
  pod="${STATEFULSET}-${ordinal}"
  ready=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
  [ "${ready}" = "True" ] || return 1

  current_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  if [ -n "${source_pod_uid}" ]; then
    [ -n "${current_pod_uid}" ] && [ "${current_pod_uid}" != "${source_pod_uid}" ]
    return
  fi

  # State schema v1 did not record the source Pod UID. Its concrete consumer
  # is an interrupted pre-0.4.7 rollout: a later hook replaced the voter but
  # left the older `restarting` record behind. ControllerRevision.revision is
  # the controller-owned monotonic order that proves the current Ready Pod is
  # newer than that saved target. Remove this branch once releases predating
  # state schema v2 are no longer supported upgrade sources.
  current_revision=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.labels.controller-revision-hash}' 2>/dev/null || true)
  saved_sequence=$(kubectl -n "${NAMESPACE}" get controllerrevision "${saved_revision}" \
    -o jsonpath='{.revision}' 2>/dev/null || true)
  current_sequence=$(kubectl -n "${NAMESPACE}" get controllerrevision "${current_revision}" \
    -o jsonpath='{.revision}' 2>/dev/null || true)
  case "${saved_sequence}" in
    ''|*[!0-9]*)
      return 1
      ;;
  esac
  case "${current_sequence}" in
    ''|*[!0-9]*)
      return 1
      ;;
  esac
  [ "${current_sequence}" -gt "${saved_sequence}" ]
}

finish_recovery_restart() {
  ordinal=$1
  node_id=$2
  wait_for_pod_ready "${ordinal}"
  if ! pod_matches_target "${ordinal}" "${TARGET_REVISION}"; then
    log "recovered node ${node_id} did not start at ${TARGET_IMAGE}@${TARGET_REVISION}"
    return 1
  fi
  start_forward "${ordinal}"
  repair_restarted_voter "${node_id}"
  "${CTL}" wait \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --stall-timeout-secs 300 \
    --ready-timeout-secs 1800 \
    --lag-tolerance 16
  finish_prepared_restart "${node_id}"
  strict_verify
  record_state complete "${node_id}"
}

finish_superseded_replacement() {
  ordinal=$1
  node_id=$2
  wait_for_pod_ready "${ordinal}"
  start_forward "${ordinal}"
  repair_restarted_voter "${node_id}"
  "${CTL}" wait \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --stall-timeout-secs 300 \
    --ready-timeout-secs 1800 \
    --lag-tolerance 16
  finish_prepared_restart "${node_id}"
  strict_verify
  record_state complete "${node_id}"
}

resume_quiesce_upgrade() {
  ordinal=$1
  node_id=$2
  source_pod_uid=$3
  pod="${STATEFULSET}-${ordinal}"
  current_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  if [ -n "${current_pod_uid}" ] && [ "${current_pod_uid}" = "${source_pod_uid}" ]; then
    log "replacing drained legacy node ${node_id} with a restart-quiesce-capable binary"
    replace_pod "${ordinal}"
  fi
  wait_for_pod_ready "${ordinal}"
  if ! pod_matches_target "${ordinal}" "${TARGET_REVISION}"; then
    log "legacy recovery handoff at node ${node_id} produced a non-target Pod"
    return 1
  fi
  log "node ${node_id} now supports durable learner repair; rebuilding its incomplete groups"
  finish_recovery_restart "${ordinal}" "${node_id}"
}

resume_if_needed() {
  if ! kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" >/dev/null 2>&1; then
    return 0
  fi
  saved_image=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.target-image}')
  saved_revision=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.target-revision}')
  state_schema=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.state-schema-version}' 2>/dev/null || true)
  source_pod_uid=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.source-pod-uid}' 2>/dev/null || true)
  phase=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.phase}')
  node_id=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.node-id}')
  case "${phase}" in
    complete)
      return 0
      ;;
    restarting|upgrading-restart-quiesce)
      ;;
    *)
      log "unsupported rollout state phase: ${phase}"
      return 1
      ;;
  esac
  case "${state_schema:-1}" in
    1)
      if [ "${phase}" != "restarting" ]; then
        log "rollout state schema 1 cannot represent phase ${phase}"
        return 1
      fi
      ;;
    2)
      if [ -z "${source_pod_uid}" ]; then
        log "rollout state schema 2 is missing source-pod-uid"
        return 1
      fi
      ;;
    *)
      log "unsupported rollout state schema: ${state_schema}"
      return 1
      ;;
  esac
  case "${node_id}" in
    ''|*[!0-9]*)
      log "invalid saved rollout node id: ${node_id}"
      return 1
      ;;
  esac
  if [ "${node_id}" -lt 1 ] || [ "${node_id}" -gt "${REPLICAS}" ]; then
    log "saved rollout node id ${node_id} is outside 1..${REPLICAS}"
    return 1
  fi
  ordinal=$((node_id - 1))
  TARGET_REVISION=$(desired_revision)
  log "resuming interrupted rollout at node ${node_id}: schema=${state_schema:-1} saved=${saved_image}@${saved_revision} current=${TARGET_IMAGE}@${TARGET_REVISION}"
  if replacement_attempt_was_superseded "${ordinal}" "${saved_revision}" "${source_pod_uid}"; then
    log "saved replacement at node ${node_id} was superseded by a newer Ready Pod; reconciling its durable membership"
    finish_superseded_replacement "${ordinal}" "${node_id}"
    return 0
  fi
  if [ "${phase}" = "upgrading-restart-quiesce" ]; then
    resume_quiesce_upgrade "${ordinal}" "${node_id}" "${source_pod_uid}"
    return
  fi
  if ! pod_matches_target "${ordinal}" "${TARGET_REVISION}"; then
    # The concrete compatibility consumer is an interrupted rollout whose
    # partial replacement predates restart quiescence. Drain it without
    # invoking an endpoint it does not have, persist the handoff, replace it
    # with the target binary, then rebuild it through durable membership.
    # Remove this phase after every retained rollout state and running voter
    # is known to include the restart-quiesce endpoint.
    start_forward "${ordinal}"
    prepare_recovery_handoff "${node_id}"
    legacy_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-${ordinal}" \
      -o jsonpath='{.metadata.uid}')
    record_state upgrading-restart-quiesce "${node_id}" "${legacy_pod_uid}"
    resume_quiesce_upgrade "${ordinal}" "${node_id}" "${legacy_pod_uid}"
    return
  fi
  # A recorded restart owns this drained node even if the previous Job died
  # before or after quiescence. Recreate the source process at most once, then
  # normalize every unready group through detach -> learner -> voter. The
  # membership repair is durable and idempotent, so no process-local token is
  # required to infer how far the previous attempt got.
  current_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-${ordinal}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  if [ -n "${current_pod_uid}" ] && [ "${current_pod_uid}" = "${source_pod_uid}" ]; then
    log "recreating recorded restart source at node ${node_id}"
    replace_pod "${ordinal}"
  fi
  finish_recovery_restart "${ordinal}" "${node_id}"
}

recover_amnesiac_if_needed() {
  node_id=$("${CTL}" classify-amnesiac \
    --config "${MANIFEST}" \
    --lag-tolerance 16)
  if [ "${node_id}" = "none" ]; then
    return 0
  fi
  case "${node_id}" in
    ''|*[!0-9]*)
      log "invalid amnesiac recovery node id: ${node_id}"
      return 1
      ;;
  esac
  if [ "${node_id}" -lt 1 ] || [ "${node_id}" -gt "${REPLICAS}" ]; then
    log "amnesiac recovery node id ${node_id} is outside 1..${REPLICAS}"
    return 1
  fi
  ordinal=$((node_id - 1))
  TARGET_REVISION=$(desired_revision)
  log "preparing uniquely classified amnesiac voter ${node_id} for revision ${TARGET_REVISION}"
  source_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-${ordinal}" \
    -o jsonpath='{.metadata.uid}')
  record_state restarting "${node_id}" "${source_pod_uid}"
  PREPARED_RESTART_NODE=${node_id}
  "${CTL}" prepare-amnesiac-restart \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --drain-timeout-secs 300 \
    --http-timeout-secs 60 \
    --lag-tolerance 16
  replace_pod "${ordinal}"
  wait_for_pod_ready "${ordinal}"
  if ! pod_matches_target "${ordinal}" "${TARGET_REVISION}"; then
    log "recovered node ${node_id} did not start at ${TARGET_IMAGE}@${TARGET_REVISION}"
    return 1
  fi
  start_forward "${ordinal}"
  repair_restarted_voter "${node_id}"
  "${CTL}" wait \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --stall-timeout-secs 300 \
    --ready-timeout-secs 1800 \
    --lag-tolerance 16
  finish_prepared_restart "${node_id}"
  strict_verify
  record_state complete "${node_id}"
  log "amnesiac voter ${node_id} recovered and verified"
}

roll_node() {
  ordinal=$1
  node_id=$((ordinal + 1))
  pod="${STATEFULSET}-${ordinal}"
  TARGET_REVISION=$(desired_revision)
  if [ -z "${TARGET_REVISION}" ]; then
    log "StatefulSet has no update revision"
    return 1
  fi
  image=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.spec.containers[?(@.name=="ursula")].image}')
  revision=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.labels.controller-revision-hash}')
  if pod_matches_target "${ordinal}" "${TARGET_REVISION}"; then
    log "node ${node_id} already runs ${TARGET_IMAGE} revision ${TARGET_REVISION}; verifying"
    strict_verify
    return 0
  fi

  log "draining node ${node_id} before ${image}@${revision} -> ${TARGET_IMAGE}@${TARGET_REVISION}"
  strict_verify
  if ! capability=$(restart_quiesce_capability "${node_id}"); then
    return 1
  fi
  if [ "${capability}" = "legacy-unavailable" ]; then
    # Ursula 0.4.8 is the concrete compatibility consumer. Its admin plane
    # has no restart-quiesce route, so make a fail-closed memory-WAL handoff,
    # persist the source UID, replace it once, and rebuild durable membership.
    # Remove this branch after no running voter or retained rollout state can
    # reference 0.4.8.
    log "node ${node_id} predates restart quiescence; preparing durable recovery handoff"
    prepare_recovery_handoff "${node_id}"
    source_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
      -o jsonpath='{.metadata.uid}')
    record_state upgrading-restart-quiesce "${node_id}" "${source_pod_uid}"
    resume_quiesce_upgrade "${ordinal}" "${node_id}" "${source_pod_uid}"
    return
  fi
  "${CTL}" drain \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --drain-timeout-secs 300 \
    --ready-timeout-secs 300 \
    --lag-tolerance 16
  source_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.uid}')
  record_state restarting "${node_id}" "${source_pod_uid}"
  PREPARED_RESTART_NODE=${node_id}
  "${CTL}" prepare-restart \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --http-timeout-secs 60

  replace_pod "${ordinal}"

  wait_for_pod_ready "${ordinal}"
  start_forward "${ordinal}"
  image=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.spec.containers[?(@.name=="ursula")].image}')
  if [ "${image}" != "${TARGET_IMAGE}" ]; then
    log "node ${node_id} recreated with unexpected image ${image}"
    return 1
  fi
  revision=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.labels.controller-revision-hash}')
  current_revision=$(desired_revision)
  if [ "${revision}" != "${current_revision}" ]; then
    log "node ${node_id} recreated at revision ${revision} while target moved to ${current_revision}; finishing recovery before another pass"
  fi

  repair_restarted_voter "${node_id}"
  "${CTL}" wait \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --stall-timeout-secs 300 \
    --ready-timeout-secs 1800 \
    --lag-tolerance 16
  finish_prepared_restart "${node_id}"
  strict_verify
  TARGET_REVISION=${current_revision}
  record_state complete "${node_id}"
  log "node ${node_id} verified"
}

main() {
  write_manifest
  wait_for_template

  # Open tunnels only for voters that are already serving, then recover a
  # replacement recorded as in-flight before requiring every pod to be Ready.
  # The opposite order makes a superseded, crash-looping replacement
  # impossible for the rollout state machine itself to repair.
  start_ready_forwards
  resume_if_needed

  ordinal=0
  while [ "${ordinal}" -lt "${REPLICAS}" ]; do
    wait_for_pod_ready "${ordinal}"
    start_forward "${ordinal}"
    ordinal=$((ordinal + 1))
  done

  recover_amnesiac_if_needed
  strict_verify

  pass=1
  while :; do
    TARGET_REVISION=$(desired_revision)
    pass_revision=${TARGET_REVISION}
    log "starting convergence pass ${pass} for revision ${pass_revision}"
    ordinal=$((REPLICAS - 1))
    while [ "${ordinal}" -ge 0 ]; do
      roll_node "${ordinal}"
      ordinal=$((ordinal - 1))
    done

    strict_verify
    TARGET_REVISION=$(desired_revision)
    if [ "${TARGET_REVISION}" = "${pass_revision}" ] &&
       all_pods_match_target "${TARGET_REVISION}"; then
      break
    fi
    pass=$((pass + 1))
    if [ "${pass}" -gt 5 ]; then
      log "StatefulSet template did not converge after 5 passes: started=${pass_revision} current=${TARGET_REVISION}"
      return 1
    fi
    log "template changed or pods remain stale; retrying against revision ${TARGET_REVISION}"
  done

  record_state complete 0
  log "graceful rollout complete: ${TARGET_IMAGE}@${TARGET_REVISION}"
}

if [ "${ROLLOUT_SOURCE_ONLY:-0}" != "1" ]; then
  main "$@"
fi
