//! The meta group's control plane: desired placement of every data group,
//! the lifecycle of every node id, and the retirements a converged move owes.
//!
//! The crate is pure data plus deterministic state transitions: no I/O, no
//! async and no clock reads. Each group's committed membership stays the only
//! truth of where it lives now, and its leader converges it toward the
//! placement here.
//!
//! Module map:
//!
//! - `placement`: the kernel and its planners. The state, the seven
//!   commands and their pure dispatcher, the seed rule, placement views, the
//!   membership reconciler's planner, the hosting rules with the teardown
//!   steps, the decommission planner and the admin JSON types. Everything
//!   is re-exported here, together with the shared vocabulary of
//!   [`ursula_proto::placement`].
//! - `samples` (tests and the `test-support` feature only): one sample of
//!   every variant of each kernel root, nested variants included, with
//!   distinct field values, for the MessagePack fixtures of other crates.

mod placement;

pub use placement::Admission;
pub use placement::BlockReason;
pub use placement::BlockedMove;
pub use placement::BootstrapProblem;
pub use placement::Cluster;
pub use placement::ClusterParams;
pub use placement::ClusterView;
pub use placement::ConfirmedStamp;
pub use placement::ConfirmedView;
pub use placement::ControlApplied;
pub use placement::ControlCommand;
pub use placement::ControlError;
pub use placement::ControlPlaneState;
pub use placement::ControlResponse;
pub use placement::Counts;
pub use placement::DecommissionMove;
pub use placement::DecommissionPlan;
pub use placement::EffectiveMembership;
pub use placement::GroupPlacement;
pub use placement::HistoryBegun;
pub use placement::HistoryCause;
pub use placement::HostingInput;
pub use placement::HostingPlan;
pub use placement::HostingStep;
pub use placement::LeaderCandidates;
pub use placement::LeaderView;
pub use placement::Leadership;
pub use placement::LifecycleChange;
pub use placement::LocalLog;
pub use placement::LocalReplica;
pub use placement::MaintenanceAuthority;
pub use placement::MembershipConfig;
pub use placement::MembershipStep;
pub use placement::MembershipTarget;
pub use placement::MembershipView;
pub use placement::MetaLeaderReport;
pub use placement::Move;
pub use placement::MoveLimit;
pub use placement::MoveLimitRequest;
pub use placement::MoveRequest;
pub use placement::NodeEligibility;
pub use placement::PeerGate;
pub use placement::PlacementApiError;
pub use placement::PlacementApiReason;
pub use placement::PlacementView;
pub use placement::Placements;
pub use placement::Registration;
pub use placement::Retirement;
pub use placement::SeedRoleOf;
pub use placement::Standing;
pub use placement::Target;
pub use placement::TargetStage;
pub use placement::Teardown;
pub use placement::TeardownEnd;
pub use placement::TeardownEvent;
pub use placement::TeardownOutcome;
pub use placement::TeardownStep;
pub use placement::UnconfirmedView;
pub use placement::ViewFreshness;
pub use placement::ViewInput;
pub use placement::default_meta_voters;
pub use placement::effective_replication_factor;
pub use placement::free_slots;
pub use placement::group_ids;
pub use placement::initializer;
pub use placement::leader_candidates;
pub use placement::membership_answer;
pub use placement::plan_decommission;
pub use placement::plan_hosting;
pub use placement::plan_membership_step;
#[cfg(any(test, feature = "test-support"))]
pub use placement::samples;
pub use placement::seed_role;
pub use placement::seed_voters;
pub use ursula_proto::placement::EmptyNodeSet;
pub use ursula_proto::placement::GenesisClaim;
pub use ursula_proto::placement::GroupCount;
pub use ursula_proto::placement::LearnerLag;
pub use ursula_proto::placement::MembershipAnswer;
pub use ursula_proto::placement::MembershipLogId;
pub use ursula_proto::placement::MembershipNodes;
pub use ursula_proto::placement::MetaLogId;
pub use ursula_proto::placement::NodeId;
pub use ursula_proto::placement::NodeLifecycle;
pub use ursula_proto::placement::NodeRecord;
pub use ursula_proto::placement::NodeSet;
pub use ursula_proto::placement::PlacementEpoch;
pub use ursula_proto::placement::RaftLogId;
pub use ursula_proto::placement::ReconcileState;
pub use ursula_proto::placement::ReconcileStatus;
pub use ursula_proto::placement::Registered;
pub use ursula_proto::placement::ReplicaRole;
pub use ursula_proto::placement::ReplicationFactor;
pub use ursula_proto::placement::StepFailure;
pub use ursula_proto::placement::StepFailureKind;
pub use ursula_proto::placement::StepKind;
pub use ursula_proto::placement::TeardownStage;
pub use ursula_proto::placement::ViewStage;
pub use ursula_proto::placement::VoterSet;
pub use ursula_proto::placement::WaitReason;
pub use ursula_proto::placement::WriteEffect;
