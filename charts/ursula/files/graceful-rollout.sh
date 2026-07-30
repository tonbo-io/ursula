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
trap stop_forwards EXIT INT TERM

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
  state_file=/tmp/rollout-state.yaml
  kubectl -n "${NAMESPACE}" create configmap "${STATE_CONFIGMAP}" \
    --from-literal=target-image="${TARGET_IMAGE}" \
    --from-literal=target-revision="${TARGET_REVISION}" \
    --from-literal=phase="${phase}" \
    --from-literal=node-id="${node_id}" \
    --dry-run=client -o yaml >"${state_file}"
  kubectl -n "${NAMESPACE}" apply -f "${state_file}"
}

resume_if_needed() {
  if ! kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" >/dev/null 2>&1; then
    return 0
  fi
  saved_image=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.target-image}')
  phase=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.phase}')
  node_id=$(kubectl -n "${NAMESPACE}" get configmap "${STATE_CONFIGMAP}" \
    -o jsonpath='{.data.node-id}')
  if [ "${saved_image}" != "${TARGET_IMAGE}" ] || [ "${phase}" != "restarting" ]; then
    return 0
  fi
  ordinal=$((node_id - 1))
  log "resuming interrupted rollout at node ${node_id}"
  wait_for_pod_ready "${ordinal}"
  image=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-${ordinal}" \
    -o jsonpath='{.spec.containers[?(@.name=="ursula")].image}')
  if [ "${image}" != "${TARGET_IMAGE}" ]; then
    log "node ${node_id} was not replaced before interruption; safely restarting its rollout"
    record_state pending "${node_id}"
    return 0
  fi
  start_forward "${ordinal}"
  "${CTL}" wait \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --stall-timeout-secs 300 \
    --ready-timeout-secs 1800 \
    --lag-tolerance 16
  "${CTL}" undrain --config "${MANIFEST}" --node "${node_id}"
  strict_verify
  record_state complete "${node_id}"
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
  "${CTL}" prepare-restart --config "${MANIFEST}" --node "${node_id}"
  record_state restarting "${node_id}"

  old_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}')
  stop_forward "${ordinal}"
  kubectl -n "${NAMESPACE}" delete pod "${pod}" --wait=false
  attempts=0
  while :; do
    new_uid=$(kubectl -n "${NAMESPACE}" get pod "${pod}" \
      -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    if [ -n "${new_uid}" ] && [ "${new_uid}" != "${old_uid}" ]; then
      break
    fi
    attempts=$((attempts + 1))
    [ "${attempts}" -lt 300 ]
    sleep 1
  done

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

  "${CTL}" wait \
    --config "${MANIFEST}" \
    --node "${node_id}" \
    --stall-timeout-secs 300 \
    --ready-timeout-secs 1800 \
    --lag-tolerance 16
  "${CTL}" undrain --config "${MANIFEST}" --node "${node_id}"
  strict_verify
  TARGET_REVISION=${current_revision}
  record_state complete "${node_id}"
  log "node ${node_id} verified"
}

main() {
  write_manifest
  wait_for_template

  ordinal=0
  while [ "${ordinal}" -lt "${REPLICAS}" ]; do
    wait_for_pod_ready "${ordinal}"
    start_forward "${ordinal}"
    ordinal=$((ordinal + 1))
  done

  resume_if_needed
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
