//! Operational library behind the `ursulactl` CLI.
//!
//! ursulactl manages logical cluster state over Ursula's admin and metrics
//! HTTP APIs: leadership placement, readiness, and membership repair. It
//! executes nothing on hosts. Physical lifecycle (deploying, restarting, and
//! scaling processes) belongs to the platform, such as Helm and OpenTofu on
//! Kubernetes or systemd on hosts, and the operator brings their own tunnel
//! (for example `kubectl port-forward`) when the loopback-bound admin plane
//! must be reached from outside.
//!
//! Module map:
//!
//! - [`metrics`]: HTTP client for `/__ursula/metrics` and the admin endpoints.
//! - [`observe`]: read-only status and cluster-wide readiness reporting.
//! - [`plan`]: pure drain planning and per-node readiness checks.
//! - [`backup`]: verifiable cluster backup, verification, and restore.
//! - [`maintenance`]: node-maintenance verbs (drain, restart repair, catch-up).
//! - [`provider`]: cluster manifest loading and node addressing.

pub mod backup;
pub mod maintenance;
pub mod metrics;
pub mod observe;
pub mod plan;
pub mod provider;

pub use maintenance::CatchUpOptions;
pub use maintenance::CatchUpOutcome;
pub use maintenance::DrainOptions;
pub use maintenance::DrainOutcome;
pub use maintenance::MembershipRepairOptions;
pub use maintenance::RestartPreparation;
pub use maintenance::abort_prepared_restart;
pub use maintenance::drain_node;
pub use maintenance::finish_prepared_restart;
pub use maintenance::prepare_amnesiac_restart;
pub use maintenance::prepare_recovery_handoff;
pub use maintenance::prepare_restart;
pub use maintenance::repair_restarted_voter;
pub use maintenance::undrain_node;
pub use maintenance::wait_cluster_ready;
pub use maintenance::wait_node_ready;
pub use metrics::ClusterSnapshot;
pub use metrics::MetricsClient;
pub use metrics::RaftGroupView;
pub use metrics::RestartQuiesceCapability;
pub use observe::StatusReport;
pub use observe::wait_ready;
pub use observe::write_status;
pub use plan::AmnesiacVoter;
pub use plan::DrainPlan;
pub use plan::GroupTransfer;
pub use plan::ReadinessReport;
pub use plan::classify_amnesiac_voter;
pub use provider::NodeInfo;
pub use provider::NodeProvider;
pub use provider::StaticNodeProvider;
