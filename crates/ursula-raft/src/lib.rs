//! OpenRaft integration for Ursula.
//!
//! Module map:
//! - `replica_fence`: durable group-local replica admission.
//!
//! - [`apply_failure`]: typed group-local fatal application failures.
//! - [`types`]: shared `UrsulaRaftTypeConfig`, type aliases, and the
//!   [`RaftGroupResponse`] applied-entry response around the canonical
//!   `ursula_runtime` write types.
//! - [`codec`]: serde (MessagePack) wire helpers for the canonical command,
//!   response, and error types that travel through the Raft state machine.
//! - [`grpc`]: gRPC service ([`RaftGrpcService`]) and network factory
//!   ([`GrpcRaftNetworkFactory`]) used for inter-node Raft RPCs.
//! - [`log_store`]: the Raft log store, the only Raft WAL. It writes the
//!   shared per-core journal through an I/O seam whose `cfg(madsim)`
//!   implementation is the simulated disk (`SimDisk`), keeps votes and each
//!   group's log state ([`wal::GroupLogState`]) in per-core metadata files, and
//!   records each run of the node in a run-state file that decides how the
//!   journals reopen ([`wal::WalOpening`]).
//! - [`wal`]: the single public WAL facade, format types, and simulation adapters.
//! - [`election`]: shared campaign and leadership transfer policy.
//! - [`recovery_transport`]: native transport adapter for recovery probes.
//! - [`registry_error`]: typed registry errors and gRPC status classification.
//! - [`registry`]: [`RaftGroupHandleRegistry`] and the single-node network.
//! - `in_process`: fault-injection network, compiled only for tests and madsim.
//! - [`owner`]: mailbox dispatch onto each group owner runtime.
//! - [`maintenance`]: configuration-backed local Raft maintenance eligibility.
//! - [`state_machine`]: per-group [`RaftGroupStateMachine`] and snapshot builder.
//! - [`meta`]: durable control state, committed watches and coalesced leader reads.
//! - [`meta_disk`]: fsync-always control log and checksummed snapshot persistence.
//! - [`meta_transport`]: independent meta replication and leader-forwarded reads/writes.
//! - [`engine`]: [`RaftGroupEngine`] + `GroupEngine` impl, with the engine
//!   factories under `engine::factory`.
//! - [`format_epoch`]: the Raft protocol (format-epoch) peer probe and the
//!   mismatch record readiness reads.
//! - [`forward`]: leader-forwarding helpers used by the engine when a node is a follower.
//! - [`snapshot_cadence`]: the byte-based snapshot cadence policy (F12e) and
//!   the per-group log gauges the snapshot driver reads.
//! - [`snapshot_references`]: prepared external-pointer pins and recoverable
//!   current-reference publication outside RaftCore.
//! - [`rejoin`]: the recovery gate of a replica that may be missing entries
//!   it acknowledged, the leader-side heal driver, and the bootstrap probe.
//! - [`snapshot_codec`]: the group-snapshot frame codec; [`group_snapshot_frames`]
//!   and [`decode_group_snapshot`] are re-exported for measurement tools.

#[expect(
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    reason = "tonic-build emits #[allow] attributes in generated code"
)]
pub mod raft_internal_proto {
    tonic::include_proto!("ursula.raft.v1");
}

mod apply_failure;
#[cfg(all(test, not(madsim)))]
mod apply_failure_tests;
mod owner;
pub use owner::OwnerRaftHandle;
mod codec;
mod election;
mod engine;
mod format_epoch;
mod forward;
mod grpc;
#[cfg(any(test, madsim))]
mod in_process;
mod log_store;
mod maintenance;
mod meta;
mod meta_disk;
#[cfg(all(test, not(madsim)))]
mod meta_durable_tests;
mod meta_transport;
mod read_index;
mod recovery_transport;
mod registry;
mod replica_fence;
pub use replica_fence::ReplicaFenceError;
mod registry_error;
mod rejoin;
mod rt;
#[cfg(madsim)]
mod sim_runtime;
pub mod snapshot_cadence;
mod snapshot_codec;
mod snapshot_references;
mod state_machine;
mod telemetry;
mod types;
pub mod wal;

pub use election::ElectionPolicy;
pub use engine::DurableRaftGroupEngineFactory;
pub use engine::RaftEngineConfig;
pub use engine::RaftGroupEngine;
pub use engine::RaftGroupEngineOptions;
pub use engine::StaticGrpcRaftGroupEngineFactory;
pub use format_epoch::FormatEpochMismatch;
pub use format_epoch::PeerFormatEpoch;
pub use format_epoch::probe_peer_format_epoch;
pub use grpc::CoreRaftTransport;
pub use grpc::GrpcRaftNetwork;
pub use grpc::GrpcRaftNetworkFactory;
pub use grpc::QuorumPrefix;
pub use grpc::RAFT_GRPC_APPEND_PATH;
pub use grpc::RAFT_GRPC_APPEND_STREAM_PATH;
pub use grpc::RAFT_GRPC_FULL_SNAPSHOT_PATH;
pub use grpc::RAFT_GRPC_GROUP_READ_PATH;
pub use grpc::RAFT_GRPC_GROUP_WRITE_PATH;
pub use grpc::RAFT_GRPC_MAX_MESSAGE_BYTES;
pub use grpc::RAFT_GRPC_REJOIN_BARRIER_PATH;
pub use grpc::RAFT_GRPC_TRANSFER_LEADER_PATH;
pub use grpc::RAFT_GRPC_VOTE_PATH;
pub use grpc::RaftGrpcMetricsSnapshot;
pub use grpc::RaftGrpcService;
#[cfg(test)]
pub(crate) use grpc::confirm_quorum_prefix;
pub use grpc::raft_grpc_metrics_snapshot;
pub use grpc::raft_grpc_service;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftFaultAction;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftFaultScript;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftFaultStep;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftNetwork;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftNetworkEvent;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftNetworkFactory;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftNetworkPolicy;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftNetworkPolicyEvent;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftRegistry;
#[cfg(any(test, madsim))]
pub use in_process::InProcessRaftRpcKind;
pub use maintenance::RaftMaintenanceIssue;
pub use maintenance::RaftMaintenanceReport;
pub use maintenance::check_raft_maintenance;
pub use meta::MetaNodeRegistration;
pub use meta::MetaRaft;
pub use meta::MetaRaftError;
pub use meta::MetaRaftHandle;
pub use meta::MetaRaftSnapshotBuilder;
pub use meta::MetaRaftStateMachine;
pub use meta::MetaRaftTypeConfig;
pub use meta_disk::MetaDiskLogStore;
pub use meta_transport::MetaGrpcNetworkFactory;
pub use meta_transport::MetaGrpcService;
pub use meta_transport::meta_peer_initialized;
pub use registry::LeadershipShedFlag;
pub use registry::LeadershipShedReason;
pub use registry::LeadershipShedState;
pub use registry::LeadershipTransferError;
pub use registry::QuorumProofError;
pub use registry::RaftGroupHandle;
pub use registry::RaftGroupHandleRegistry;
pub use registry::SingleNodeRaftNetwork;
pub use registry::SingleNodeRaftNetworkFactory;
pub use registry_error::RegistryError;
pub use registry_error::SnapshotStage;
pub use rejoin::AcceptUnsyncedLossOutcome;
pub use rejoin::AcceptUnsyncedLossReport;
pub use rejoin::GroupBootstrap;
pub use rejoin::GroupRejoin;
pub use rejoin::PeerGroupLog;
pub use rejoin::RECOVERY_STALL_AFTER;
pub use rejoin::RecoveryConfig;
pub use rejoin::RecoveryGate;
pub use rejoin::RecoveryGateError;
pub use rejoin::RecoveryGateStatus;
pub use rejoin::RecoveryMembershipAuthority;
pub use rejoin::RecoveryTransport;
pub use rejoin::bootstrap_probe_vote;
pub use rejoin::run_group_bootstrap;
pub use rejoin::run_rejoin_heal;
pub use rejoin::run_rejoin_vote_barrier;
#[cfg(madsim)]
pub use sim_runtime::MadsimOpenRaftRuntime;
pub use snapshot_codec::decode_group_snapshot;
pub use snapshot_codec::group_snapshot_frames;
pub use state_machine::RaftGroupSnapshotBuilder;
pub use state_machine::RaftGroupStateMachine;
pub use state_machine::SnapshotBuildCoordinator;
pub use types::RaftGroupMaintenanceState;
pub use types::RaftGroupMetricsSnapshot;
pub use types::RaftGroupResponse;
pub use types::RaftLogProgressSnapshot;
pub use types::StaticGrpcRaftMembershipConfig;
pub use types::UrsulaAppendEntriesRequest;
pub use types::UrsulaAppendEntriesResponse;
pub use types::UrsulaRaftTypeConfig;
pub use types::UrsulaVote;
pub use types::UrsulaVoteRequest;
pub use types::UrsulaVoteResponse;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod cold_index_tests;

pub use meta_transport::MetaRecoveryStatus;
pub use meta_transport::meta_authorize_genesis;
pub use meta_transport::meta_peer_recovery_floor;
pub use meta_transport::meta_peer_recovery_status;

#[cfg(all(test, not(madsim)))]
mod process_fence_measurement;

pub use meta_transport::MetaRpcAuth;
pub use meta_transport::meta_authorize_genesis_authenticated;
pub use meta_transport::meta_peer_initialized_authenticated;
pub use meta_transport::meta_peer_recovery_floor_authenticated;
pub use meta_transport::meta_peer_recovery_status_authenticated;
