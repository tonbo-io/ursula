//! OpenRaft integration for Ursula.
//!
//! Module map:
//! - [`apply_failure`]: fatal per-group application diagnostics.
//!
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
//!   group's log state ([`wal::diagnostics::GroupLogState`]) in per-core metadata files, and
//!   records each run of the node in a run-state file that decides how the
//!   journals reopen ([`WalOpening`]).
//! - [`wal`]: node WAL facade and separately named format diagnostics.
//! - [`election`]: shared campaign and leadership transfer policy.
//! - [`recovery_transport`]: native transport adapter for recovery probes.
//! - [`registry`]: [`RaftGroupHandleRegistry`] and the single-node network.
//! - `in_process`: fault-injection network, compiled only for tests and madsim.
//! - [`owner`]: mailbox dispatch onto each group owner runtime.
//! - [`leadership`]: one replica's view of its group's leadership, which
//!   routing and forward hints share.
//! - [`maintenance`]: configuration-backed local Raft maintenance eligibility.
//! - [`state_machine`]: per-group [`RaftGroupStateMachine`] and snapshot builder.
//! - [`meta`]: meta-group OpenRaft type config and control-plane state machine.
//! - [`engine`]: [`RaftGroupEngine`] + `GroupEngine` impl, with the engine
//!   factories under `engine::factory`.
//! - [`openraft_error`]: the one conversion from OpenRaft's errors to the
//!   group engine's error.
//! - [`openraft_work`]: the work OpenRaft runs for a group engine on tasks of
//!   its own, which the engine's shutdown waits for (#709).
//! - [`format_epoch`]: the Raft protocol (format-epoch) peer probe and the
//!   mismatch record readiness reads.
//! - [`forward`]: leader-forwarding helpers used by the engine when a node is a follower.
//! - [`peer_cluster`]: the cluster-id check between peers: the identity
//!   every Raft gRPC request carries, the server interceptor that refuses a
//!   peer of another cluster, and the record readiness and metrics read.
//! - [`peer_channel`]: the endpoint policy (connect timeout, HTTP/2 and TCP
//!   keepalive) of every gRPC channel a node opens to a peer.
//! - [`snapshot_cadence`]: the byte-based snapshot cadence policy (F12e) and
//!   the per-group log gauges the snapshot driver reads.
//! - [`snapshot_references`]: prepared external-pointer pins, recoverable
//!   current-reference publication outside RaftCore, and the pins and GC
//!   passes that delete the snapshot objects a replica replaced (#669).
//! - [`history`]: history identity on a group: the leader's fence while
//!   the group's history is pending and its obligation to propose the
//!   history event (genesis, recovery plans).
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

mod leadership;
mod owner;
pub use leadership::GroupLeadership;
pub use leadership::LeaderRole;
pub use owner::OwnerRaftHandle;
#[cfg(any(test, feature = "fault-injection"))]
pub use owner::StepDown;
#[cfg(any(test, feature = "fault-injection"))]
pub use owner::StepDownError;
mod codec;
mod election;
mod engine;
mod format_epoch;
mod forward;
mod grpc;
mod history;
#[cfg(any(test, madsim))]
mod in_process;
mod log_store;
mod maintenance;
mod meta;
mod openraft_error;
mod openraft_work;
mod peer_channel;
mod peer_cluster;
mod read_index;
mod recovery_transport;
mod registry;
mod rejoin;
mod rt;
#[cfg(madsim)]
mod sim_runtime;
#[cfg(madsim)]
mod sim_watch;
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
pub use grpc::RaftGrpcServer;
pub use grpc::RaftGrpcService;
#[cfg(all(test, not(madsim)))]
pub(crate) use grpc::confirm_quorum_prefix;
pub use grpc::raft_grpc_metrics_snapshot;
pub use grpc::raft_grpc_server;
pub use grpc::raft_grpc_service;
pub use history::GroupHistory;
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
pub use log_store::JournalTuning;
pub use log_store::LaggingGroups;
pub use log_store::PlanCut;
pub use log_store::RaftGroupFileLogStore;
pub use log_store::RaftWal;
pub use log_store::RaftWalError;
pub use log_store::RecoveryRecord;
pub use log_store::RecoveryState;
pub use log_store::SnapshotOrderError;
pub use log_store::SnapshotRecord;
pub use log_store::WalMaintenance;
pub use log_store::WalOpening;
pub use log_store::generate_cluster_id;
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
pub use meta::MetaSnapshot;
pub use meta::MetaSnapshotData;
pub use meta::UnexpectedRegistrationResponse;
pub use peer_cluster::CLUSTER_ID_METADATA;
pub use peer_cluster::ClusterCheck;
pub use peer_cluster::NODE_ID_METADATA;
pub use peer_cluster::PEER_CLUSTER_MISMATCH_TTL;
pub use peer_cluster::PeerCluster;
pub use peer_cluster::PeerClusterError;
pub use peer_cluster::PeerClusterMismatch;
pub use peer_cluster::PeerIdentity;
pub use registry::IncomingTransferError;
pub use registry::LeadershipShedFlag;
pub use registry::LeadershipShedReason;
pub use registry::LeadershipShedState;
pub use registry::LeadershipTransferError;
pub use registry::QuorumProofError;
pub use registry::RaftGroupHandle;
pub use registry::RaftGroupHandleRegistry;
pub use registry::SelfElectionError;
pub use registry::SingleNodeRaftNetwork;
pub use registry::SingleNodeRaftNetworkFactory;
#[cfg(any(test, feature = "fault-injection"))]
pub use registry::SnapshotBuildError;
pub use registry::SnapshotGcPass;
pub use registry::SnapshotInstallError;
pub use rejoin::GateOpenError;
pub use rejoin::GroupBootstrap;
pub use rejoin::GroupRejoin;
pub use rejoin::PeerGroupLog;
pub use rejoin::PlanAnswer;
pub use rejoin::RECOVERY_STALL_AFTER;
pub use rejoin::RecoveryConfig;
pub use rejoin::RecoveryGate;
pub use rejoin::RecoveryGateError;
pub use rejoin::RecoveryGateStatus;
pub use rejoin::RecoveryTransport;
pub use rejoin::ReplicaRestart;
pub use rejoin::bootstrap_probe_vote;
pub use rejoin::leads_live;
pub use rejoin::run_group_bootstrap;
pub use rejoin::run_rejoin_vote_barrier;
#[cfg(madsim)]
pub use sim_runtime::MadsimOpenRaftRuntime;
#[cfg(madsim)]
pub use sim_runtime::run_isolated;
pub use snapshot_codec::decode_group_snapshot;
pub use snapshot_codec::group_snapshot_frames;
pub use state_machine::BuildsChanged;
pub use state_machine::DEFAULT_SNAPSHOT_GC_GRACE;
pub use state_machine::GroupSnapshotWork;
pub use state_machine::Handoff;
pub use state_machine::PurgeBusy;
pub use state_machine::PurgeHoldGuard;
pub use state_machine::RaftGroupSnapshotBuilder;
pub use state_machine::RaftGroupStateMachine;
pub use state_machine::RaftMetricsWatch;
pub use state_machine::SNAPSHOT_HANDOFF_WAIT;
pub use state_machine::SnapshotBuildCoordinator;
pub use state_machine::SnapshotPermitError;
pub use state_machine::SnapshotStartError;
pub use types::RaftGroupMaintenanceState;
pub use types::RaftGroupMetricsSnapshot;
pub use types::RaftGroupResponse;
pub use types::RaftLogProgressSnapshot;
pub use types::SnapshotData;
pub use types::SnapshotLease;
pub use types::StaticGrpcRaftMembershipConfig;
pub use types::UrsulaAppendEntriesRequest;
pub use types::UrsulaAppendEntriesResponse;
pub use types::UrsulaRaftTypeConfig;
pub use types::UrsulaSnapshot;
pub use types::UrsulaVote;
pub use types::UrsulaVoteRequest;
pub use types::UrsulaVoteResponse;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod cold_index_tests;

pub mod apply_failure;
#[cfg(test)]
#[cfg(not(madsim))]
mod apply_failure_tests;
