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
//! - [`log_store`]: in-memory and durable file-backed Raft log stores.
//! - [`registry`]: [`RaftGroupHandleRegistry`] and the single-node test network.
//! - [`state_machine`]: per-group [`RaftGroupStateMachine`] and snapshot builder.
//! - [`engine`]: [`RaftGroupEngine`], the engine factories, and `GroupEngine` impl.
//! - [`forward`]: leader-forwarding helpers used by the engine when a node is a follower.

pub mod raft_internal_proto {
    tonic::include_proto!("ursula.raft.v1");
}

mod codec;
mod engine;
mod forward;
mod grpc;
mod log_store;
mod registry;
mod state_machine;
mod types;

pub use engine::{
    ColdRaftGroupEngineFactory, DurableRaftGroupEngineFactory, DurableRaftLogStoreFactory,
    RaftGroupEngine, RaftGroupEngineFactory, RegisteredRaftGroupEngineFactory,
    StaticGrpcRaftGroupEngineFactory,
};
pub use grpc::{
    GrpcRaftNetwork, GrpcRaftNetworkFactory, RAFT_GRPC_APPEND_PATH,
    RAFT_GRPC_FORWARD_HTTP_WRITE_PATH, RAFT_GRPC_FULL_SNAPSHOT_PATH, RAFT_GRPC_GROUP_READ_PATH,
    RAFT_GRPC_GROUP_WRITE_PATH, RAFT_GRPC_MAX_MESSAGE_BYTES, RAFT_GRPC_VOTE_PATH, RaftGrpcService,
    raft_grpc_service,
};
pub use log_store::{RaftGroupFileLogStore, RaftGroupLogStore};
pub use registry::{RaftGroupHandleRegistry, SingleNodeRaftNetwork, SingleNodeRaftNetworkFactory};
pub use state_machine::{RaftGroupSnapshotBuilder, RaftGroupStateMachine};
pub use types::{
    RaftGroupCommand, RaftGroupMetricsSnapshot, RaftGroupResponse, RaftLogProgressSnapshot,
    UrsulaAppendEntriesRequest, UrsulaAppendEntriesResponse, UrsulaRaftTypeConfig, UrsulaVote,
    UrsulaVoteRequest, UrsulaVoteResponse,
};

#[cfg(test)]
mod tests;
