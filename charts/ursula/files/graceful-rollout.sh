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
      # Metrics use the same Pod-bound tunnel. Keep http_url as the real peer
      # address: learner attachment persists it in Raft membership.
      printf '      "metrics_url": "http://127.0.0.1:%s",\n' "$((FORWARD_PORT_BASE + ordinal))"
      printf '      "host": "%s-%s",\n' "${STATEFULSET}" "${ordinal}"
      printf '      "http_url": "http://%s-%s.%s-headless.%s.svc.cluster.local:%s"\n' \
        "${STATEFULSET}" "${ordinal}" "${STATEFULSET}" "${NAMESPACE}" "${CLIENT_PORT}"
      printf '    }'
      ordinal=$((ordinal + 1))
    done
    printf '\n  ]\n}\n'
  } >"${MANIFEST}"
}

# Pin across separate CLI invocations and persist the plan before a mutation.
# --allow-legacy-incarnation is only the deployed <=0.6.2 upgrade consumer;
# those entries remain explicitly uncertified until each voter is replaced.
pin_manifest() {
  if ! "${CTL}" pin-incarnations --config "${MANIFEST}" \
      --allow-legacy-incarnation --http-timeout-secs 60 >"${MANIFEST}.next"; then
    rm -f "${MANIFEST}.next"
    return 1
  fi
  mv "${MANIFEST}.next" "${MANIFEST}" || return 1
}

bind_replacement_incarnation() {
  node_id=$1
  saved_uid=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.source-pod-uid}') || return 1
  saved_schema=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.state-schema-version}') || return 1
  bound_uid=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.replacement-pod-uid}') || return 1
  current_uid=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-$((node_id - 1))" \
    -o jsonpath='{.metadata.uid}') || return 1
  case "${saved_schema:-1}" in
    1|2|3) ;;
    *) log "unsupported replacement state schema: ${saved_schema}"; return 1 ;;
  esac
  if [ "${saved_schema}" = 3 ] && { [ -z "${saved_uid}" ] || [ "${saved_uid}" = "${current_uid}" ]; }; then
    log "refusing to refresh node ${node_id} process without an admitted Pod replacement"
    return 1
  fi
  [ -n "${current_uid}" ] || return 1
  if [ -n "${bound_uid}" ]; then
    if [ "${bound_uid}" != "${current_uid}" ]; then
      log "replacement Pod changed after its process plan was saved; refusing another identity refresh"
      return 1
    fi
    # Same Pod with a restarted container must retain the old process pin and
    # fail closed. A resumed Job cannot turn that restart into new authority.
    pin_manifest
    return
  fi
  if ! "${CTL}" pin-incarnations --config "${MANIFEST}" --replace-node "${node_id}" \
      --allow-legacy-incarnation --http-timeout-secs 60 >"${MANIFEST}.next"; then
    rm -f "${MANIFEST}.next"
    return 1
  fi
  mv "${MANIFEST}.next" "${MANIFEST}" || return 1
  # Schema 1/2 are interrupted legacy rollouts without a saved instance plan.
  # Preserve their admitted UID when present. Do not create a schema-3 state
  # without a source UID; the old schema-1 path finishes only this migration.
  if [ -n "${saved_uid}" ]; then
    record_state restarting "${node_id}" "${saved_uid}" "${current_uid}"
  fi
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

cleanup() {
  stop_forwards
}
trap cleanup EXIT INT TERM

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

wait_for_pod_started() {
  ordinal=$1
  pod="${STATEFULSET}-${ordinal}"
  # Ready certifies a caught-up Raft voter. Wait for the TCP startup probe
  # here so the admin plane is reachable while the node catches up.
  kubectl -n "${NAMESPACE}" wait \
    --for='jsonpath={.status.containerStatuses[?(@.name=="ursula")].started}=true' \
    "pod/${pod}" --timeout=15m
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
  old_uid=${2:?replacement requires the admitted source Pod UID}
  pod="${STATEFULSET}-${ordinal}"
  # Never refresh the admitted identity by name here: another actor may have
  # already replaced it. The API's UID precondition rejects that race without
  # deleting the new voter. Preserve the Pod's configured termination grace.
  case "${old_uid}" in
    *[!0-9a-f-]*) log "invalid source Pod UID"; return 1 ;;
  esac
  [ "${#old_uid}" -eq 36 ] || return 1
  stop_forward "${ordinal}"
  if ! printf '{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":"%s"}}\n' "${old_uid}" \
    | kubectl delete --raw="/api/v1/namespaces/${NAMESPACE}/pods/${pod}" -f - >/dev/null; then
    return 1
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

record_state() {
  phase=$1
  node_id=$2
  source_pod_uid=${3:-}
  replacement_pod_uid=${4:-}
  state_file=/tmp/rollout-state.yaml
  kubectl -n "${NAMESPACE}" create configmap "${STATE_CONFIGMAP}" \
    --from-literal=state-schema-version="3" \
    --from-literal=target-image="${TARGET_IMAGE}" \
    --from-literal=target-revision="${TARGET_REVISION}" \
    --from-literal=phase="${phase}" \
    --from-literal=node-id="${node_id}" \
    --from-literal=source-pod-uid="${source_pod_uid}" \
    --from-file=process-manifest="${MANIFEST}" \
    --from-literal=replacement-pod-uid="${replacement_pod_uid}" \
    --dry-run=client -o yaml >"${state_file}" || return 1
  kubectl -n "${NAMESPACE}" apply -f "${state_file}" || return 1
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

# The replacement starts maintenance-drained: the entrypoint reads the
# restarting state. A replica that lost Raft log entries is gated and rebuilt
# by its group leaders, so the rollout only waits until the node is a
# caught-up voter in every group before it clears the drain.
rejoin_node() {
  ordinal=$1
  node_id=$2
  bind_replacement_incarnation "${node_id}" || return 1
  "${CTL}" wait \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --stall-timeout-secs 300 \
    --ready-timeout-secs 1800 \
    --lag-tolerance 16
  "${CTL}" undrain \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --http-timeout-secs 60
  wait_for_pod_ready "${ordinal}"
  strict_verify
}

finish_restart() {
  ordinal=$1
  node_id=$2
  wait_for_pod_started "${ordinal}"
  if ! pod_matches_target "${ordinal}" "${TARGET_REVISION}"; then
    log "restarted node ${node_id} did not start at ${TARGET_IMAGE}@${TARGET_REVISION}"
    return 1
  fi
  start_forward "${ordinal}"
  rejoin_node "${ordinal}" "${node_id}"
  record_state complete "${node_id}"
}

finish_superseded_replacement() {
  ordinal=$1
  node_id=$2
  wait_for_pod_started "${ordinal}"
  start_forward "${ordinal}"
  rejoin_node "${ordinal}" "${node_id}"
  record_state complete "${node_id}"
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
    restarting)
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
    3)
      if [ -z "${source_pod_uid}" ]; then
        log "rollout state schema 3 is missing source-pod-uid"
        return 1
      fi
      # Read the durable plan before any state-changing CLI operation. Parsing
      # and all surviving instance matches are checked by pin-incarnations.
      kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
        -o jsonpath='{.data.process-manifest}' >"${MANIFEST}"
      if [ ! -s "${MANIFEST}" ]; then
        log "rollout state schema 3 is missing process-manifest"
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
    log "saved replacement at node ${node_id} was superseded by a newer Ready Pod; waiting for it to catch up"
    finish_superseded_replacement "${ordinal}" "${node_id}"
    return 0
  fi
  # A recorded restart owns this drained node even if the previous Job died
  # before or after the replacement. Recreate the source process at most
  # once. A replacement created from an older template started drained too
  # (the entrypoint reads the restarting state), so replace it with the
  # current target before it rejoins.
  current_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-${ordinal}" \
    -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
  if [ -n "${current_pod_uid}" ] && [ "${current_pod_uid}" = "${source_pod_uid}" ]; then
    log "recreating recorded restart source at node ${node_id}"
    replace_pod "${ordinal}" "${source_pod_uid}" || return 1
  elif [ -n "${current_pod_uid}" ] && ! pod_matches_target "${ordinal}" "${TARGET_REVISION}"; then
    log "replacing non-target node ${node_id} recorded as restarting"
    replace_pod "${ordinal}" "${current_pod_uid}" || return 1
  fi
  finish_restart "${ordinal}" "${node_id}"
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
  "${CTL}" drain \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --drain-timeout-secs 300 \
    --ready-timeout-secs 300 \
    --lag-tolerance 16
  source_pod_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
    -o jsonpath='{.metadata.uid}')
  record_state restarting "${node_id}" "${source_pod_uid}"

  replace_pod "${ordinal}" "${source_pod_uid}" || return 1

  wait_for_pod_started "${ordinal}"
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
    log "node ${node_id} recreated at revision ${revision} while target moved to ${current_revision}; finishing this restart before another pass"
  fi

  rejoin_node "${ordinal}" "${node_id}"
  TARGET_REVISION=${current_revision}
  record_state complete "${node_id}"
  log "node ${node_id} verified"
}

legacy_shared_store_guard() {
  # Once a reviewed shared reservation exists, a later values/default regression
  # must not re-enable this independent writer, even when the store is idle.
  # Bootstrap itself requires no legacy executor still running.
  if ! shared_store=$(kubectl -n "${NAMESPACE}" get configmap "${STATEFULSET}-maintenance" \
      --ignore-not-found=true -o name); then
    log "cannot establish whether shared maintenance is enabled; refusing legacy rollout"
    return 1
  fi
  if [ -n "${shared_store}" ]; then
    log "persistent maintenance store exists; enable gracefulRollout.maintenanceReservation"
    return 1
  fi
}

main() {
  legacy_shared_store_guard || return 1
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

  pin_manifest || return 1
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
