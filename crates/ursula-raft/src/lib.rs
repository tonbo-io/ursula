//! OpenRaft integration for Ursula.
//!
//! Module map:
//!
//! - [`types`]: shared `UrsulaRaftTypeConfig`, type aliases, and the
//!   protobuf-backed [`RaftGroupCommand`]/[`RaftGroupResponse`] wire types.
//! - [`codec`]: protobuf <-> Rust-domain conversions for every engine value
//!   that travels through the Raft state machine.
//! - [`grpc`]: gRPC service ([`RaftGrpcService`]) and network factory
//!   ([`GrpcRaftNetworkFactory`]) used for inter-node Raft RPCs.
//! - [`log_store`]: in-memory and durable file-backed Raft log stores (see
//!   `log_store::memory` and `log_store::file`).
//! - [`registry`]: [`RaftGroupHandleRegistry`] and the single-node test network.
//! - [`state_machine`]: per-group [`RaftGroupStateMachine`] and snapshot builder.
//! - [`meta`]: meta-group OpenRaft type config and control-plane state machine.
//! - [`engine`]: [`RaftGroupEngine`] + `GroupEngine` impl, with the engine
//!   factories under `engine::factory`.
//! - [`forward`]: leader-forwarding helpers used by the engine when a node is a follower.

pub mod raft_internal_proto {
    tonic::include_proto!("ursula.raft.v1");
}

mod codec;
mod engine;
mod forward;
mod grpc;
mod log_store;
mod meta;
mod registry;
mod rt;
#[cfg(madsim)]
mod sim_runtime;
mod snapshot_codec;
mod state_machine;
mod telemetry;
mod types;

pub use engine::ColdRaftGroupEngineFactory;
pub use engine::DurableRaftGroupEngineFactory;
pub use engine::DurableRaftLogStoreFactory;
pub use engine::RaftEngineConfig;
pub use engine::RaftGroupEngine;
pub use engine::RaftGroupEngineFactory;
pub use engine::RegisteredRaftGroupEngineFactory;
pub use engine::StaticGrpcRaftGroupEngineFactory;
pub use grpc::GrpcRaftNetwork;
pub use grpc::GrpcRaftNetworkFactory;
pub use grpc::RAFT_GRPC_APPEND_PATH;
pub use grpc::RAFT_GRPC_FULL_SNAPSHOT_PATH;
pub use grpc::RAFT_GRPC_GROUP_READ_PATH;
pub use grpc::RAFT_GRPC_GROUP_WRITE_PATH;
pub use grpc::RAFT_GRPC_MAX_MESSAGE_BYTES;
pub use grpc::RAFT_GRPC_TRANSFER_LEADER_PATH;
pub use grpc::RAFT_GRPC_VOTE_PATH;
pub use grpc::RaftGrpcService;
pub use grpc::raft_grpc_service;
pub use log_store::MemoryRaftLogStore;
pub use log_store::MetaRaftLogStore;
pub use log_store::RaftGroupFileLogStore;
pub use log_store::RaftGroupLogStore;
pub use meta::MetaNodeRegistration;
pub use meta::MetaRaft;
pub use meta::MetaRaftError;
pub use meta::MetaRaftHandle;
pub use meta::MetaRaftSnapshotBuilder;
pub use meta::MetaRaftStateMachine;
pub use meta::MetaRaftTypeConfig;
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
#[cfg(madsim)]
pub use sim_runtime::MadsimOpenRaftRuntime;
pub use state_machine::RaftGroupSnapshotBuilder;
pub use state_machine::RaftGroupStateMachine;
pub use types::RaftGroupCommand;
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
