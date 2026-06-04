//! Pure fault-plan / SimEvent introspection helpers extracted from
//! `madsim_harness/mod.rs` (DoD #3 modularity refactor). These are tiny
//! `has_*_in_fault_plan` / `*_from_fault_plan` predicates that the dispatch
//! layer + minimize tool use to decide which scenario / invariant matches.

use super::{
    ColdStoreEvent, ColdStoreOperation, HttpProtocolSurfacePlan, InProcessRaftNetworkEvent,
    InProcessRaftNetworkPolicyEvent, RuntimeInterleavingPlan, RuntimeRaftNetworkWorkloadPlan,
    SimEvent, SimFaultAction, SimFaultPlan, SimTrace, duration_ms, network_rpc_kind_name,
};

pub(super) fn runtime_interleaving_plan_from_fault_plan(
    fault_plan: &SimFaultPlan,
) -> Option<RuntimeInterleavingPlan> {
    fault_plan.steps.iter().find_map(|step| match &step.action {
        SimFaultAction::RunRuntimeSeededInterleaving { plan } => Some(plan.clone()),
        _ => None,
    })
}

pub(super) fn runtime_raft_network_workload_plan_from_fault_plan(
    fault_plan: &SimFaultPlan,
) -> Option<RuntimeRaftNetworkWorkloadPlan> {
    fault_plan.steps.iter().find_map(|step| match &step.action {
        SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } => Some(plan.clone()),
        _ => None,
    })
}

pub(super) fn http_protocol_surface_plan_from_fault_plan(
    fault_plan: &SimFaultPlan,
) -> Option<HttpProtocolSurfacePlan> {
    fault_plan.steps.iter().find_map(|step| match &step.action {
        SimFaultAction::RunHttpProtocolSurfaceWorkload { plan } => Some(plan.clone()),
        _ => None,
    })
}

pub(super) fn corrupt_cold_live_read_node_from_fault_plan(
    fault_plan: &SimFaultPlan,
) -> Option<u64> {
    fault_plan.steps.iter().find_map(|step| match &step.action {
        SimFaultAction::CorruptColdLiveReadExpectation { node_id } => Some(*node_id),
        _ => None,
    })
}

pub(super) fn cold_read_truncate_len_from_fault_plan(fault_plan: &SimFaultPlan) -> Option<usize> {
    fault_plan.steps.iter().find_map(|step| match &step.action {
        SimFaultAction::TruncateNextColdRead { returned_len } => Some(*returned_len),
        _ => None,
    })
}

pub(super) fn cold_read_delay_ms_from_fault_plan(fault_plan: &SimFaultPlan) -> Option<u64> {
    fault_plan.steps.iter().find_map(|step| match &step.action {
        SimFaultAction::DelayNextColdRead { delay_ms } => Some(*delay_ms),
        _ => None,
    })
}

pub(super) fn cold_write_delay_ms_from_fault_plan(fault_plan: &SimFaultPlan) -> Option<u64> {
    fault_plan.steps.iter().find_map(|step| match &step.action {
        SimFaultAction::DelayNextColdWrite { delay_ms } => Some(*delay_ms),
        _ => None,
    })
}

pub(super) fn has_cold_read_fault_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::FailNextColdRead))
}

pub(super) fn has_cold_write_fault_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::FailNextColdWrite))
}

pub(super) fn has_retry_cold_write_after_failure_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::RetryColdWriteAfterFailure))
}

pub(super) fn has_retry_cold_read_after_failure_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::RetryColdReadAfterFailure))
}

pub(super) fn has_cold_delete_fault_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::FailNextColdDelete))
}

pub(super) fn has_partition_seeded_follower_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::PartitionSeededFollower))
}

pub(super) fn has_heal_seeded_follower_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::HealSeededFollower))
}

pub(super) fn has_verify_runtime_cold_live_reads_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::VerifyRuntimeColdLiveReads))
}

pub(super) fn has_stop_seeded_follower_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::StopSeededFollower))
}

pub(super) fn has_restart_stopped_follower_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::RestartStoppedFollower))
}

pub(super) fn has_stop_current_leader_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::StopCurrentLeader))
}

pub(super) fn has_restart_stopped_leader_in_fault_plan(fault_plan: &SimFaultPlan) -> bool {
    fault_plan
        .steps
        .iter()
        .any(|step| matches!(step.action, SimFaultAction::RestartStoppedLeader))
}

pub(super) fn has_corrupt_runtime_raft_snapshot_append_counts_in_fault_plan(
    fault_plan: &SimFaultPlan,
) -> bool {
    fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            SimFaultAction::CorruptRuntimeRaftSnapshotAppendCounts
        )
    })
}

pub(super) fn has_corrupt_http_producer_duplicate_expectation_in_fault_plan(
    fault_plan: &SimFaultPlan,
) -> bool {
    fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            SimFaultAction::CorruptHttpProducerDuplicateExpectation
        )
    })
}

pub(super) fn has_corrupt_http_live_sse_next_offset_expectation_in_fault_plan(
    fault_plan: &SimFaultPlan,
) -> bool {
    fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            SimFaultAction::CorruptHttpLiveSseNextOffsetExpectation
        )
    })
}

pub(super) fn has_corrupt_http_live_limit_backpressure_expectation_in_fault_plan(
    fault_plan: &SimFaultPlan,
) -> bool {
    fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            SimFaultAction::CorruptHttpLiveLimitBackpressureExpectation
        )
    })
}

pub(super) fn has_corrupt_http_snapshot_body_expectation_in_fault_plan(
    fault_plan: &SimFaultPlan,
) -> bool {
    fault_plan.steps.iter().any(|step| {
        matches!(
            step.action,
            SimFaultAction::CorruptHttpSnapshotBodyExpectation
        )
    })
}

pub(super) fn sim_event_from_cold_store_event(event: ColdStoreEvent) -> SimEvent {
    match event {
        ColdStoreEvent::WriteChunkBegin { path, payload_len } => {
            SimEvent::ColdObjectWriteBegin { path, payload_len }
        }
        ColdStoreEvent::WriteChunkComplete { path, object_size } => {
            SimEvent::ColdObjectWriteComplete { path, object_size }
        }
        ColdStoreEvent::DeleteChunkBegin { path } => SimEvent::ColdObjectDeleteBegin { path },
        ColdStoreEvent::DeleteChunkComplete { path } => SimEvent::ColdObjectDeleteComplete { path },
        ColdStoreEvent::RemoveAllBegin { path } => SimEvent::ColdObjectRemoveAllBegin { path },
        ColdStoreEvent::RemoveAllComplete { path } => {
            SimEvent::ColdObjectRemoveAllComplete { path }
        }
        ColdStoreEvent::ReadObjectRangeBegin {
            stream_id,
            path,
            read_start_offset,
            len,
            object_start,
            object_end,
            cached,
        } => SimEvent::ColdObjectReadBegin {
            stream: stream_id,
            path,
            read_start_offset,
            len,
            object_start,
            object_end,
            cached,
        },
        ColdStoreEvent::ReadObjectRangeComplete {
            stream_id,
            path,
            read_start_offset,
            len,
            returned_len,
            cached,
        } => SimEvent::ColdObjectReadComplete {
            stream: stream_id,
            path,
            read_start_offset,
            len,
            returned_len,
            cached,
        },
        ColdStoreEvent::FaultInjected {
            operation,
            stream_id,
            path,
            message,
        } => SimEvent::ColdStoreFaultInjected {
            operation: cold_store_operation_name(operation).to_owned(),
            stream: stream_id,
            path,
            message,
        },
        ColdStoreEvent::DelayInjected {
            operation,
            stream_id,
            path,
            delay_ms,
        } => SimEvent::ColdStoreDelayInjected {
            operation: cold_store_operation_name(operation).to_owned(),
            stream: stream_id,
            path,
            delay_ms,
        },
        ColdStoreEvent::TruncateInjected {
            stream_id,
            path,
            requested_len,
            returned_len,
        } => SimEvent::ColdStoreTruncateInjected {
            stream: stream_id,
            path,
            requested_len,
            returned_len,
        },
    }
}

pub(super) fn cold_store_operation_name(operation: ColdStoreOperation) -> &'static str {
    match operation {
        ColdStoreOperation::WriteChunk => "write_chunk",
        ColdStoreOperation::DeleteChunk => "delete_chunk",
        ColdStoreOperation::RemoveAll => "remove_all",
        ColdStoreOperation::ReadObjectRange => "read_object_range",
    }
}

pub(super) fn sim_event_from_network_event(event: InProcessRaftNetworkEvent) -> SimEvent {
    match event {
        InProcessRaftNetworkEvent::PolicyChanged { action } => {
            let (action, source, target, delay_ms) = network_policy_action_parts(action);
            SimEvent::NetworkPolicyChanged {
                action,
                source,
                target,
                delay_ms,
            }
        }
        InProcessRaftNetworkEvent::RpcDecision {
            source,
            target,
            kind,
            delay,
            partitioned,
        } => SimEvent::NetworkRpcDecision {
            source,
            target,
            kind: network_rpc_kind_name(kind).to_owned(),
            delay_ms: delay.map(duration_ms),
            partitioned,
        },
        InProcessRaftNetworkEvent::RpcDelivered {
            source,
            target,
            kind,
        } => SimEvent::NetworkRpcDelivered {
            source,
            target,
            kind: network_rpc_kind_name(kind).to_owned(),
        },
        InProcessRaftNetworkEvent::RpcMissingTarget {
            source,
            target,
            kind,
        } => SimEvent::NetworkRpcMissingTarget {
            source,
            target,
            kind: network_rpc_kind_name(kind).to_owned(),
        },
    }
}

pub(super) fn network_policy_action_parts(
    action: InProcessRaftNetworkPolicyEvent,
) -> (String, Option<u64>, Option<u64>, Option<u64>) {
    match action {
        InProcessRaftNetworkPolicyEvent::SetDelay(delay) => {
            ("set_delay".to_owned(), None, None, delay.map(duration_ms))
        }
        InProcessRaftNetworkPolicyEvent::PartitionOneWay { source, target } => (
            "partition_one_way".to_owned(),
            Some(source),
            Some(target),
            None,
        ),
        InProcessRaftNetworkPolicyEvent::PartitionBidirectional { a, b } => {
            ("partition_bidirectional".to_owned(), Some(a), Some(b), None)
        }
        InProcessRaftNetworkPolicyEvent::HealOneWay { source, target } => {
            ("heal_one_way".to_owned(), Some(source), Some(target), None)
        }
        InProcessRaftNetworkPolicyEvent::HealBidirectional { a, b } => {
            ("heal_bidirectional".to_owned(), Some(a), Some(b), None)
        }
        InProcessRaftNetworkPolicyEvent::Clear => ("clear".to_owned(), None, None, None),
    }
}

pub(super) fn invariant_failed(trace: &SimTrace, invariant: &str) -> bool {
    trace.events.iter().any(|event| {
        matches!(
            event,
            SimEvent::InvariantFailed {
                invariant: candidate,
                ..
            } if candidate == invariant
        )
    })
}

pub(super) fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else {
        "non-string panic payload".to_owned()
    }
}
