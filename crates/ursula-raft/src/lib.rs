//! OpenRaft integration for Ursula.
//!
//! Module map:
//! - [`apply_failure`]: fatal per-group application diagnostics.
//!
//! - [`types`]: shared `UrsulaRaftTypeConfig`, type aliases, the
//!   [`RaftCommand`] of every entry on either plane, and the
//!   [`RaftGroupResponse`] applied-entry response around the canonical
//!   `ursula_runtime` write types.
//! - [`meta`]: the meta group: its replicated types (its commands, its
//!   state, its snapshot pointer and its genesis probe), its state machine,
//!   this node's meta replica and the placement views it publishes, and the
//!   meta runtime that forms the group, registers the node and bootstraps
//!   the cluster.
//! - [`target`]: the typed target of every Raft RPC (plane, group and target
//!   node), its one encoder and parser, and the misdelivery refusal.
//! - [`memberships`]: the memberships a recovery barrier answers with, which
//!   every asker classifies with `ursula_control::membership_answer`.
//! - [`codec`]: serde (MessagePack) wire helpers for the canonical command,
//!   response, and error types that travel through the Raft state machine.
//! - [`grpc`]: gRPC service ([`RaftGrpcService`]) and network factory
//!   ([`GrpcRaftNetworkFactory`]) used for inter-node Raft RPCs.
//! - [`log_store`]: the Raft log store, the only Raft WAL. It writes the
//!   shared per-core journal through an I/O seam whose `cfg(madsim)`
//!   implementation is the simulated disk (`SimDisk`), keeps votes and each
//!   group's log state ([`wal::diagnostics::GroupLogState`]) in per-core metadata files, and
//!   records each run of the node in a run-state file that decides how the
//!   journals reopen ([`WalOpening`], and [`MetaOpening`] for the meta
//!   group's own journal).
//! - [`wal`]: node WAL facade and separately named format diagnostics.
//! - [`election`]: shared campaign and leadership transfer policy.
//! - [`recovery_transport`]: native transport adapter for recovery probes.
//! - [`registry`]: [`RaftGroupHandleRegistry`] and the single-node network.
//! - `in_process`: fault-injection network, generic over the state machine,
//!   compiled only for tests and madsim.
//! - [`owner`]: mailbox dispatch onto each group owner runtime.
//! - [`leadership`]: one replica's view of its group's leadership, which
//!   routing and forward hints share.
//! - [`maintenance`]: local Raft maintenance eligibility, against the node's
//!   placement view and each replica's effective membership.
//! - [`state_machine`]: per-group [`RaftGroupStateMachine`] and snapshot builder.
//! - [`engine`]: [`RaftGroupEngine`] + `GroupEngine` impl, with the placed
//!   engine factory under `engine::factory`.
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
//! - [`incarnation`]: the process fence on Raft RPCs: the table of the
//!   peers' processes, the receiver's check and the sender's reading of an
//!   answer's stamp.
//! - [`predecessor_wait`]: the wait a run owes a predecessor that may
//!   still run, and the leadership shed of the node fence.
//! - [`peer_directory`]: where each configured peer listens (`raft.peers`),
//!   the one source every reader of a peer address takes it from.
//! - `placement_wiring`: the one seam of every placement input of a data
//!   replica's open ([`PlacementInputs`]): admission, the seed role and the
//!   node sets its recovery drivers read, from meta ([`MetaPlacement`]).
//! - [`peer_channel`]: the endpoint policy (connect timeout, HTTP/2 and TCP
//!   keepalive) of every gRPC channel a node opens to a peer.
//! - [`snapshot_cadence`]: the byte-based snapshot cadence policy (F12e) and
//!   the per-group log gauges the snapshot driver reads.
//! - [`snapshot_holds`]: what one engine incarnation of a group may still
//!   use of its own snapshot objects, held in memory, and the GC pass that
//!   deletes every other object in the node's directory (#687, #669).
//! - [`hosting`]: the hosting driver, the one owner of what a node runs:
//!   it plans over the journals and the placement view, writes the
//!   [`HostingSet`] every open is admitted by, and opens and closes
//!   replicas through a [`GroupHost`].
//! - [`history`]: history identity on a group: the leader's fence while
//!   the group's history is pending and its obligation to propose the
//!   history event (genesis, recovery plans).
//! - [`rejoin`]: the recovery gate of a replica that may be missing entries
//!   it acknowledged, the membership reconcile shell around the pure
//!   membership planner ([`MembershipTargetSource`], [`PeerGates`]), which
//!   every replica runs and a leader reports its group's convergence
//!   through, and the bootstrap probe.
//! - [`replica`]: one replica of either plane: the gate screens of every
//!   inbound RPC, the recovery barrier with its history fence, and the
//!   recovery plan's orchestration, generic over the state machine.
//! - [`snapshot_codec`]: the group-snapshot frame codec; [`group_snapshot_frames`]
//!   and [`decode_group_snapshot`] are re-exported for measurement tools.
//! - [`voter`]: the one voter predicate on this node: whether a data
//!   replica counts as a voter, and the left mark ([`LeftMarks`]) its
//!   group's barrier answers set and its appends clear.

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
#[cfg(any(test, madsim, feature = "fault-injection"))]
pub use owner::StepDown;
#[cfg(any(test, madsim, feature = "fault-injection"))]
pub use owner::StepDownError;
mod codec;
mod election;
mod engine;
mod format_epoch;
mod forward;
mod grpc;
mod history;
pub mod hosting;
#[cfg(any(test, madsim))]
mod in_process;
mod incarnation;
mod log_store;
mod maintenance;
mod memberships;
pub mod meta;
mod openraft_error;
mod openraft_work;
mod peer_channel;
mod peer_cluster;
mod peer_directory;
mod placement_wiring;
pub mod predecessor_wait;
mod read_index;
mod recovery_transport;
mod registry;
mod rejoin;
mod replica;
mod rt;
#[cfg(madsim)]
mod sim_runtime;
#[cfg(madsim)]
mod sim_watch;
pub mod snapshot_cadence;
mod snapshot_codec;
mod snapshot_holds;
mod state_machine;
mod target;
mod telemetry;
mod types;
mod voter;
pub mod wal;

pub use election::ElectionPolicy;
pub use engine::PlacedRaftGroupEngineFactory;
pub use engine::RaftEngineConfig;
pub use engine::RaftGroupEngine;
pub use engine::RaftGroupEngineOptions;
pub use format_epoch::FormatEpochMismatch;
pub use format_epoch::PeerFormatEpoch;
pub use format_epoch::probe_peer_format_epoch;
pub use forward::ForwardError;
pub use grpc::CoreRaftTransport;
pub use grpc::GrpcRaftNetwork;
pub use grpc::GrpcRaftNetworkFactory;
pub use grpc::QuorumPrefix;
pub use grpc::RAFT_GRPC_APPEND_PATH;
pub use grpc::RAFT_GRPC_APPEND_STREAM_PATH;
pub use grpc::RAFT_GRPC_FULL_SNAPSHOT_PATH;
pub use grpc::RAFT_GRPC_GENESIS_PROBE_PATH;
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
pub use hosting::GroupHost;
pub use hosting::HostingDriver;
pub use hosting::HostingSet;
pub use hosting::HostingStatus;
pub use hosting::HostingStatusWatch;
pub use hosting::HostingTask;
#[cfg(any(test, madsim))]
pub use in_process::InProcessBarrierError;
#[cfg(any(test, madsim))]
pub use in_process::InProcessEndpoint;
#[cfg(any(test, madsim))]
pub use in_process::InProcessMetaNetworkFactory;
#[cfg(any(test, madsim))]
pub use in_process::InProcessMetaRegistry;
#[cfg(any(test, madsim))]
pub use in_process::InProcessNetwork;
#[cfg(any(test, madsim))]
pub use in_process::InProcessNetworkFactory;
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
#[cfg(any(test, madsim))]
pub use in_process::InProcessRegistry;
pub use incarnation::IncarnationCounts;
pub use log_store::CleanExit;
pub use log_store::JournalTuning;
pub use log_store::LaggingGroups;
pub use log_store::LocalGroups;
pub use log_store::MetaOpenError;
pub use log_store::MetaOpening;
pub use log_store::PROBE_INTERVAL;
pub use log_store::PlanCut;
pub use log_store::PredecessorWait;
pub use log_store::RaftGroupFileLogStore;
pub use log_store::RaftWal;
pub use log_store::RaftWalError;
pub use log_store::RecoveryRecord;
pub use log_store::RecoveryState;
pub use log_store::SnapshotOrderError;
pub use log_store::SnapshotRecord;
pub use log_store::WaitFrom;
pub use log_store::WalMaintenance;
pub use log_store::WalOpening;
pub use log_store::generate_cluster_id;
pub use maintenance::LAG_TOLERANCE;
pub use maintenance::META_UNAVAILABLE_AFTER;
pub use maintenance::MaintenanceInputs;
pub use maintenance::NodePlacement;
pub use maintenance::RaftMaintenanceIssue;
pub use maintenance::RaftMaintenanceReport;
pub use maintenance::check_raft_maintenance;
pub use maintenance::raft_maintenance_report;
pub use memberships::BarrierAnswer;
pub use memberships::BarrierAnswerRefusal;
pub use memberships::EchoedTarget;
pub use memberships::LeaderMemberships;
pub use memberships::MalformedMembership;
pub use memberships::MembershipKind;
pub use peer_cluster::BoundPeers;
pub use peer_cluster::CLUSTER_ID_METADATA;
pub use peer_cluster::ClusterCheck;
pub use peer_cluster::NODE_ID_METADATA;
pub use peer_cluster::PEER_CLUSTER_MISMATCH_TTL;
pub use peer_cluster::PeerCluster;
pub use peer_cluster::PeerClusterError;
pub use peer_cluster::PeerClusterMismatch;
pub use peer_cluster::PeerIdentity;
pub use peer_directory::PeerDirectory;
pub use peer_directory::PeerEndpoints;
pub use placement_wiring::FixedNodes;
#[cfg(any(test, madsim, feature = "test-support"))]
pub use placement_wiring::FixedPlacement;
pub use placement_wiring::FixedRole;
pub use placement_wiring::GroupNodes;
pub use placement_wiring::MetaPlacement;
#[cfg(any(test, madsim, feature = "test-support"))]
pub use placement_wiring::NoTarget;
pub use placement_wiring::PlacementInputs;
pub use placement_wiring::SeedRole;
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
pub use rejoin::Convergence;
pub use rejoin::GateOpenError;
pub use rejoin::GroupBootstrap;
pub use rejoin::GroupRejoin;
pub use rejoin::MembershipTargetSource;
pub use rejoin::NoPeerGates;
pub use rejoin::PeerGates;
pub use rejoin::PeerGroupLog;
pub use rejoin::PlanAnswer;
pub use rejoin::RECOVERY_STALL_AFTER;
pub use rejoin::RecoveryConfig;
pub use rejoin::RecoveryGate;
pub use rejoin::RecoveryGateError;
pub use rejoin::RecoveryGateStatus;
pub use rejoin::RecoveryTransport;
pub use rejoin::ReplicaRestart;
pub use rejoin::TargetRead;
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
pub use state_machine::SnapshotRecordError;
pub use state_machine::SnapshotStartError;
#[cfg(any(madsim, feature = "test-support"))]
pub use state_machine::persisted_snapshot_pointer;
#[cfg(any(madsim, feature = "test-support"))]
pub use state_machine::snapshot_record_with_pointer;
pub use target::FENCE_METADATA;
pub use target::FenceRefusal;
pub use target::INCARNATION_METADATA;
pub use target::RAFT_PLANE_METADATA;
pub use target::TARGET_INCARNATION_METADATA;
pub use target::fence_refusal;
pub use types::RaftCommand;
pub use types::RaftGroupMaintenanceState;
pub use types::RaftGroupMetricsSnapshot;
pub use types::RaftGroupResponse;
pub use types::RaftLogProgressSnapshot;
pub use types::SnapshotData;
pub use types::SnapshotLease;
pub use types::UrsulaAppendEntriesRequest;
pub use types::UrsulaAppendEntriesResponse;
pub use types::UrsulaRaftTypeConfig;
pub use types::UrsulaSnapshot;
pub use types::UrsulaVote;
pub use types::UrsulaVoteRequest;
pub use types::UrsulaVoteResponse;
pub use ursula_shard::RaftTarget;
pub use voter::LeftMarks;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod cold_index_tests;

pub mod apply_failure;
#[cfg(test)]
#[cfg(not(madsim))]
mod apply_failure_tests;
