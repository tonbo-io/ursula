//! OpenRaft integration for Ursula.
//!
//! Module map:
//!
//! - [`types`]: shared `UrsulaRaftTypeConfig`, type aliases, and the
//!   [`RaftGroupResponse`] applied-entry response around the canonical
//!   `ursula_runtime` write types.
//! - [`codec`]: serde (MessagePack) wire helpers for the canonical command,
//!   response, and error types that travel through the Raft state machine.
//! - [`grpc`]: gRPC service ([`RaftGrpcService`]) and network factory
//!   ([`GrpcRaftNetworkFactory`]) used for inter-node Raft RPCs.
//! - [`log_store`]: in-memory and durable file-backed Raft log stores (see
//!   `log_store::memory` and `log_store::file`).
//! - [`registry`]: [`RaftGroupHandleRegistry`] and the single-node test network.
//! - [`maintenance`]: configuration-backed local Raft maintenance eligibility.
//! - [`state_machine`]: per-group [`RaftGroupStateMachine`] and snapshot builder.
//! - [`membership`]: quorum-confirmed applied membership certificates for control adoption.
//! - [`meta`]: meta-group OpenRaft type config and control-plane state machine.
//! - [`meta_grpc`]: concrete meta consensus RPC service and network factory.
//! - `log_store::file`: shared data journals, owner leases and per-group WAL reclamation.
//! - `log_store::meta`: durable meta journal, snapshots, and recovery guards.
//! - `log_store::receiver`: bound receiver fences and local replica assignments.
//! - [`engine`]: [`RaftGroupEngine`] + `GroupEngine` impl, with the engine
//!   factories under `engine::factory`.
//! - [`format_epoch`]: the Raft protocol (format-epoch) peer probe and the
//!   mismatch record readiness reads.
//! - [`forward`]: leader-forwarding helpers used by the engine when a node is a follower.
//! - [`snapshot_cadence`]: the byte-based snapshot cadence policy (F12e) and
//!   the per-group log gauges the snapshot driver reads.
//! - [`snapshot_references`]: prepared external-pointer pins and recoverable
//!   current-reference publication outside RaftCore.
//! - [`rejoin`]: memory-WAL rejoin: the bootstrap probe, the vote gate of an
//!   emptied replica, and the leader-side heal driver.
//! - [`restart_guard`]: memory-WAL full-restart guard: the per-group
//!   "initialized" marker in object storage and the bootstrap decision table.
//! - [`snapshot_codec`]: the group-snapshot frame codec; [`group_snapshot_frames`]
//!   and [`decode_group_snapshot`] are re-exported for measurement tools.

pub mod raft_internal_proto {
    tonic::include_proto!("ursula.raft.v1");
}

mod codec;
mod engine;
mod format_epoch;
mod forward;
mod grpc;
mod log_store;
mod maintenance;
mod membership;
mod meta;
mod meta_grpc;
#[cfg(all(test, not(madsim)))]
mod meta_grpc_tests;
mod read_index;
mod registry;
mod rejoin;
mod restart_guard;
mod rt;
#[cfg(madsim)]
mod sim_runtime;
pub mod snapshot_cadence;
mod snapshot_codec;
mod snapshot_references;
mod state_machine;
mod telemetry;
mod types;

pub use engine::ColdRaftGroupEngineFactory;
pub use engine::DurableRaftGroupEngineFactory;
pub use engine::DurableRaftLogStoreFactory;
pub use engine::GROUP_ELECTION_TIMEOUT_MIN_MS;
pub use engine::RaftEngineConfig;
pub use engine::RaftGroupEngine;
pub use engine::RaftGroupEngineFactory;
pub use engine::RegisteredRaftGroupEngineFactory;
pub use engine::StaticGrpcRaftGroupEngineFactory;
pub use format_epoch::FormatEpochMismatch;
pub use format_epoch::PeerFormatEpoch;
pub use format_epoch::probe_peer_format_epoch;
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
pub use grpc::confirm_quorum_prefix;
pub use grpc::raft_grpc_metrics_snapshot;
pub use grpc::raft_grpc_service;
pub use grpc::request_self_election_via_transfer;
pub use log_store::ManagedReceiverStore;
pub use log_store::MemoryRaftLogStore;
pub use log_store::MetaRaftFileLogStore;
pub use log_store::MetaRaftLogStore;
pub use log_store::RaftGroupFileLogStore;
pub use log_store::RaftGroupLogStore;
#[cfg(madsim)]
pub use log_store::SimulatedReceiverDisk;
#[cfg(madsim)]
pub use log_store::SimulatedReceiverWriteFault;
pub use maintenance::ManagedRaftInventory;
pub use maintenance::ManagedReplicaRole;
pub use maintenance::RaftMaintenanceIssue;
pub use maintenance::RaftMaintenanceReport;
pub use maintenance::check_managed_raft_inventory;
pub use maintenance::check_managed_raft_maintenance;
pub use maintenance::check_raft_maintenance;
pub use membership::QuorumGroupMembership;
pub use membership::collect_bootstrap_memberships;
pub use membership::confirm_group_configuration;
pub use membership::confirm_group_membership;
pub use meta::MetaNodeRegistration;
pub use meta::MetaRaft;
pub use meta::MetaRaftError;
pub use meta::MetaRaftHandle;
pub use meta::MetaRaftSnapshotBuilder;
pub use meta::MetaRaftStateMachine;
pub use meta::MetaRaftTypeConfig;
pub use meta_grpc::META_RAFT_APPEND_PATH;
pub use meta_grpc::META_RAFT_FULL_SNAPSHOT_PATH;
pub use meta_grpc::META_RAFT_MAX_MESSAGE_BYTES;
pub use meta_grpc::META_RAFT_PROTOCOL_VERSION;
pub use meta_grpc::META_RAFT_READ_BOOTSTRAP_STATE_PATH;
pub use meta_grpc::META_RAFT_READ_PROJECTION_PATH;
pub use meta_grpc::META_RAFT_STATUS_PATH;
pub use meta_grpc::META_RAFT_TRANSFER_LEADER_PATH;
pub use meta_grpc::META_RAFT_VOTE_PATH;
pub use meta_grpc::META_RAFT_WRITE_CONTROL_PATH;
pub use meta_grpc::MetaGrpcRaftNetwork;
pub use meta_grpc::MetaGrpcRaftNetworkFactory;
pub use meta_grpc::MetaRaftGrpcService;
pub use meta_grpc::MetaReplicaStatus;
pub use meta_grpc::meta_raft_grpc_service;
pub use meta_grpc::read_bootstrap_control_state;
pub use meta_grpc::read_control_projection;
pub use meta_grpc::read_meta_replica_status;
pub use meta_grpc::write_control_command;
pub use registry::InProcessRaftFaultAction;
pub use registry::InProcessRaftFaultScript;
pub use registry::InProcessRaftFaultStep;
pub use registry::InProcessRaftNetwork;
pub use registry::InProcessRaftNetworkEvent;
pub use registry::InProcessRaftNetworkFactory;
pub use registry::InProcessRaftNetworkPolicy;
pub use registry::InProcessRaftNetworkPolicyEvent;
pub use registry::InProcessRaftRegistry;
pub use registry::InProcessRaftRpcKind;
pub use registry::LeadershipShedFlag;
pub use registry::LeadershipShedReason;
pub use registry::LeadershipShedState;
pub use registry::RaftGroupHandle;
pub use registry::RaftGroupHandleRegistry;
pub use registry::SingleNodeRaftNetwork;
pub use registry::SingleNodeRaftNetworkFactory;
pub use rejoin::AdoptSurvivorOutcome;
pub use rejoin::GroupRejoin;
pub use rejoin::PeerGroupLog;
pub use rejoin::bootstrap_probe_vote;
pub use rejoin::run_rejoin_heal;
pub use rejoin::run_rejoin_vote_barrier;
pub use restart_guard::InitMarkerStore;
pub use restart_guard::MemoryInitMarkers;
pub use restart_guard::MemoryWalBootstrap;
pub use restart_guard::RestartGuard;
pub use restart_guard::run_memory_wal_bootstrap;
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
