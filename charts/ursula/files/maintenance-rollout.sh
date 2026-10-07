#!/bin/sh
# Planned Pod replacement using a persistent, cell-wide reservation. Host loss
# needs physical fencing and is deliberately not inferred from Pod readiness.
set -eu
: "${CORE_COUNT:?}"
export ROLLOUT_SOURCE_ONLY=1
# shellcheck source=charts/ursula/files/graceful-rollout.sh
. "${ROLLOUT_LIBRARY_DIR:-$(dirname "$0")}/graceful-rollout.sh"

# Override legacy cleanup: cancellation never releases admission or drains.
trap 'stop_forwards' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
MAINTENANCE_STORE="${STATEFULSET}-maintenance"
WORK=${MAINTENANCE_WORK_DIR:-$(mktemp -d /tmp/ursula-maintenance.XXXXXX)}
CELL="${WORK}/cell.json"
SNAPSHOT="${WORK}/snapshot.json"
REQUEST="${WORK}/request.json"
FENCE="${WORK}/fence.json"
MANIFEST="${WORK}/manifest.json"

maintenance_new_id() {
  tr -d '-' </proc/sys/kernel/random/uuid
}

maintenance_read() {
  "${CTL}" reservation-read --cell "${CELL}" --snapshot "${SNAPSHOT}" --field "$@"
}

maintenance_load() {
  # Missing is an error. No bootstrap, apply, expiry, or stale-plan refresh.
  kubectl -n "${NAMESPACE}" get configmap "${MAINTENANCE_STORE}" -o json >"${SNAPSHOT}"
  maintenance_read state >/dev/null
}

maintenance_cas() {
  "${CTL}" reservation-propose --cell "${CELL}" --snapshot "${SNAPSHOT}" \
    --request "${REQUEST}" >"${WORK}/proposal.json"
  # The whole proposal retains UID/resourceVersion. Conflict or ambiguous HTTP
  # failure ends this executor; only a new executor may reconcile/take over.
  kubectl -n "${NAMESPACE}" replace -f "${WORK}/proposal.json" -o json >"${WORK}/response.json"
  "${CTL}" reservation-acknowledge --cell "${CELL}" --snapshot "${SNAPSHOT}" \
    --request "${REQUEST}" --response "${WORK}/response.json" >"${WORK}/receipt.json"
  mv "${WORK}/response.json" "${SNAPSHOT}"
}

maintenance_plan() {
  maintenance_read manifest >"${MANIFEST}"
  maintenance_read fence >"${FENCE}"
  maintenance_read manifest --exclude-source >"${WORK}/survivors.json"
}

maintenance_forward_all() {
  maintenance_forward_ordinal=0
  while [ "${maintenance_forward_ordinal}" -lt 3 ]; do
    wait_for_pod_ready "${maintenance_forward_ordinal}"
    start_forward "${maintenance_forward_ordinal}"
    maintenance_forward_ordinal=$((maintenance_forward_ordinal + 1))
  done
}

maintenance_activate() {
  "${CTL}" activate-maintenance-fence --config "$1" --http-timeout-secs 30 >/dev/null
}

maintenance_prefix() {
  # Listener startup does not establish Raft eligibility. Wait using the same
  # immutable plan before starting the freshness clock for prefix evidence.
  "${CTL}" verify-cluster --config "${MANIFEST}" --timeout-secs 300 \
    --poll-interval-secs 1 --lag-tolerance 16 >/dev/null
  # A proof older than 60 s is rejected by the policy. A slow/failed proof
  # cannot admit deletion or completion; no recovery-time optimization implied.
  "${CTL}" verify-quorum --config "${MANIFEST}" --expected-groups "${EXPECTED_GROUPS}" \
    --core-count "${CORE_COUNT}" --timeout-secs 45 --http-timeout-secs 10 >"${WORK}/observation.json"
}

maintenance_progress() {
  "${CTL}" reservation-request "$1" --fence "${FENCE}" \
    --observation "${WORK}/observation.json" >"${REQUEST}"
  maintenance_cas
}

maintenance_capture_pod() {
  kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-$((maintenance_node - 1))" \
    -o json >"${WORK}/pod.json"
  maintenance_host=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-$((maintenance_node - 1))" \
    -o jsonpath='{.spec.nodeName}')
  [ -n "${maintenance_host}" ]
  kubectl get node "${maintenance_host}" -o json >"${WORK}/node.json"
}

maintenance_reserve() {
  maintenance_node=$1
  maintenance_load
  [ "$(maintenance_read stage)" = idle ]
  maintenance_forward_all
  write_manifest
  # Observe process identities, never adopt a reported executor token.
  "${CTL}" pin-incarnations --config "${MANIFEST}" --http-timeout-secs 30 >"${WORK}/pinned.json"
  mv "${WORK}/pinned.json" "${MANIFEST}"
  maintenance_capture_pod
  "${CTL}" reservation-source --cell "${CELL}" --pod-object "${WORK}/pod.json" \
    --node-object "${WORK}/node.json" --config "${MANIFEST}" --node-id "${maintenance_node}" >"${WORK}/source.json"
  maintenance_operation=$(maintenance_new_id)
  "${CTL}" reservation-request reserve --operation-id "${maintenance_operation}" \
    --executor-id "${maintenance_executor}" --source "${WORK}/source.json" \
    --config "${MANIFEST}" >"${REQUEST}"
  maintenance_cas
  maintenance_plan
  maintenance_activate "${MANIFEST}"
  maintenance_prefix
  maintenance_progress admit-pod-deletion
}

maintenance_takeover() {
  maintenance_operation=$(maintenance_read operation-id)
  "${CTL}" reservation-request takeover --operation-id "${maintenance_operation}" \
    --executor-id "${maintenance_executor}" >"${REQUEST}"
  maintenance_cas
  maintenance_plan
}

maintenance_verify_survivors() {
  "${CTL}" verify-survivors --config "${MANIFEST}" --excluded-node-id "${maintenance_node}" \
    --expected-groups "${EXPECTED_GROUPS}" --core-count "${CORE_COUNT}" \
    --timeout-secs 45 --http-timeout-secs 10 >"${WORK}/survivor-observation.json"
}

maintenance_target_uid() {
  maintenance_uid_attempt=0
  while :; do
    maintenance_uid=$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-${maintenance_ordinal}" \
      --ignore-not-found=true -o jsonpath='{.metadata.uid}')
    if [ -n "${maintenance_uid}" ]; then
      printf '%s\n' "${maintenance_uid}"
      return 0
    fi
    maintenance_uid_attempt=$((maintenance_uid_attempt + 1))
    [ "${maintenance_uid_attempt}" -lt 300 ]
    sleep 1
  done
}

maintenance_recover_operation() {
  maintenance_node=$(maintenance_read source-node-id)
  maintenance_ordinal=$((maintenance_node - 1))
  maintenance_source_uid=$(maintenance_read source-pod-uid)
  maintenance_stage=$(maintenance_read stage)
  # Start only the two fixed survivors first. The target might be drained,
  # deleting, or awaiting Raft-aware readiness; Ready is not authority.
  maintenance_survivor=0
  while [ "${maintenance_survivor}" -lt 3 ]; do
    if [ "${maintenance_survivor}" -ne "${maintenance_ordinal}" ]; then
      wait_for_pod_started "${maintenance_survivor}"
      start_forward "${maintenance_survivor}"
    fi
    maintenance_survivor=$((maintenance_survivor + 1))
  done
  maintenance_activate "${WORK}/survivors.json"
  maintenance_current_uid=$(maintenance_target_uid)
  if [ "${maintenance_stage}" = reserved ]; then
    # No persistent admission exists; all original processes must certify.
    [ "${maintenance_current_uid}" = "${maintenance_source_uid}" ]
    wait_for_pod_started "${maintenance_ordinal}"
    start_forward "${maintenance_ordinal}"
    maintenance_activate "${MANIFEST}"
    maintenance_prefix
    maintenance_progress admit-pod-deletion
    maintenance_stage=deletion-admitted
  fi
  if [ "${maintenance_stage}" = deletion-admitted ]; then
    if [ "${maintenance_current_uid}" = "${maintenance_source_uid}" ]; then
      wait_for_pod_started "${maintenance_ordinal}"
      start_forward "${maintenance_ordinal}"
      maintenance_activate "${MANIFEST}"
      # Drain the target before deleting it. Draining an already drained
      # target is idempotent. Any failure keeps the shared operation reserved.
      "${CTL}" drain --config "${MANIFEST}" --node "${maintenance_node}" \
        --drain-timeout-secs 300 --ready-timeout-secs 300 --lag-tolerance 16
      maintenance_verify_survivors
      replace_pod "${maintenance_ordinal}" "${maintenance_source_uid}"
    else
      # An executor can have died after deletion. Binding is observation only.
      # Do not demand complete three-voter membership during the selected
      # target's automatic remove/learner/promote recovery interval.
      log "source UID already retired; resuming only its saved replacement"
    fi
    wait_for_pod_started "${maintenance_ordinal}"
    start_forward "${maintenance_ordinal}"
    # A startup-guarded server may have bound its fresh boot before listening.
    # Reload that exact operation; never adopt a new executor through this read.
    maintenance_load
    maintenance_read fence >"${WORK}/observed-fence.json"
    [ "$(cat "${FENCE}")" = "$(cat "${WORK}/observed-fence.json")" ]
    [ "$(maintenance_read source-pod-uid)" = "${maintenance_source_uid}" ]
    if [ "$(maintenance_read stage)" = replacement-bound ]; then
      [ "$(maintenance_target_uid)" = "$(maintenance_read replacement-pod-uid)" ]
    else
      [ "$(maintenance_read stage)" = deletion-admitted ]
      maintenance_capture_pod
      "${CTL}" pin-incarnations --config "${MANIFEST}" --replace-node "${maintenance_node}" \
        --http-timeout-secs 30 >"${WORK}/replacement.json"
      maintenance_bound_uid=$("${CTL}" reservation-source --cell "${CELL}" \
        --pod-object "${WORK}/pod.json" --node-object "${WORK}/node.json" \
        --config "${WORK}/replacement.json" --node-id "${maintenance_node}" --field pod-uid)
      [ -n "${maintenance_bound_uid}" ] && [ "${maintenance_bound_uid}" != "${maintenance_source_uid}" ]
      # The native policy checks the complete Pod's UID/owner, Node/provider
      # identity and target-only process change. A surrounding UID read prevents
      # pinning a process across another Pod replacement.
      [ "$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-${maintenance_ordinal}" -o jsonpath='{.metadata.uid}')" = "${maintenance_bound_uid}" ]
      "${CTL}" reservation-request bind-pod-replacement --fence "${FENCE}" \
        --pod-object "${WORK}/pod.json" --node-object "${WORK}/node.json" \
        --config "${WORK}/replacement.json" >"${REQUEST}"
      maintenance_cas
    fi
    maintenance_plan
  else
    [ "${maintenance_stage}" = replacement-bound ]
    [ "${maintenance_current_uid}" = "$(maintenance_read replacement-pod-uid)" ]
    wait_for_pod_started "${maintenance_ordinal}"
    start_forward "${maintenance_ordinal}"
  fi
  maintenance_activate "${MANIFEST}"
  # The replacement started drained. Its group leaders rebuild it if it lost
  # Raft log entries, so wait until it is a caught-up voter, then undrain it.
  "${CTL}" wait --config "${MANIFEST}" --node "${maintenance_node}" \
    --stall-timeout-secs 300 --ready-timeout-secs 1800 --lag-tolerance 16
  "${CTL}" undrain --config "${MANIFEST}" --node "${maintenance_node}" --http-timeout-secs 30
  wait_for_pod_ready "${maintenance_ordinal}"
  maintenance_prefix
  # Physical UID must remain the single bound replacement; never refresh it.
  [ "$(kubectl -n "${NAMESPACE}" get pod "${STATEFULSET}-${maintenance_ordinal}" -o jsonpath='{.metadata.uid}')" = "$(maintenance_read replacement-pod-uid)" ]
  "${CTL}" retire-maintenance-fence --config "${MANIFEST}" --http-timeout-secs 30 >/dev/null
  maintenance_prefix
  maintenance_progress complete-pod-replacement
  log "shared reservation completed for voter ${maintenance_node}; full redundancy certified"
}

maintenance_main() {
  [ "${REPLICAS}" = 3 ]
  maintenance_executor=$(maintenance_new_id)
  kubectl get namespace "${NAMESPACE}" -o json >"${WORK}/namespace.json"
  kubectl -n "${NAMESPACE}" get statefulset "${STATEFULSET}" -o json >"${WORK}/statefulset.json"
  "${CTL}" reservation-cell --namespace-object "${WORK}/namespace.json" \
    --statefulset-object "${WORK}/statefulset.json" --group-count "${EXPECTED_GROUPS}" \
    --core-count "${CORE_COUNT}" >"${CELL}"
  maintenance_load
  case "$(maintenance_read operation-kind)" in
    idle | pod-replacement) ;;
    *) log "a host recovery owns the reservation; planned Pod rollout cannot take it over"; return 1 ;;
  esac
  wait_for_template
  if [ "$(maintenance_read stage)" != idle ]; then
    maintenance_takeover
    maintenance_recover_operation
  fi
  maintenance_pass=1
  while :; do
    maintenance_revision=$(desired_revision)
    [ -n "${maintenance_revision}" ]
    maintenance_roll_ordinal=2
    while [ "${maintenance_roll_ordinal}" -ge 0 ]; do
      if ! pod_matches_target "${maintenance_roll_ordinal}" "${maintenance_revision}"; then
        maintenance_reserve "$((maintenance_roll_ordinal + 1))"
        maintenance_recover_operation
      fi
      maintenance_roll_ordinal=$((maintenance_roll_ordinal - 1))
    done
    if [ "$(desired_revision)" = "${maintenance_revision}" ] && all_pods_match_target "${maintenance_revision}"; then
      break
    fi
    maintenance_pass=$((maintenance_pass + 1))
    [ "${maintenance_pass}" -le 5 ]
  done
  maintenance_forward_all
  # A no-op rollout still requires healthy complete groups. Once an operation
  # completed, keep its fixed plan for this final read-only proof.
  if [ ! -s "${MANIFEST}" ]; then
    write_manifest
    "${CTL}" pin-incarnations --config "${MANIFEST}" --http-timeout-secs 30 >"${WORK}/pinned.json"
    mv "${WORK}/pinned.json" "${MANIFEST}"
  fi
  maintenance_prefix
  maintenance_load
  [ "$(maintenance_read stage)" = idle ]
  log "reserved graceful rollout complete: ${TARGET_IMAGE}@${maintenance_revision}"
}

if [ "${MAINTENANCE_SOURCE_ONLY:-0}" != 1 ]; then
  maintenance_main "$@"
fi
