//! Operational library behind the `ursulactl` CLI.
//!
//! ursulactl manages logical cluster state: leadership placement, readiness,
//! and rejoin permission. Physical lifecycle (deploying, restarting, and
//! scaling processes) belongs to the platform that owns it, such as OpenTofu
//! and Helm on Kubernetes or systemd on hosts.
//!
//! Module map:
//!
//! - [`metrics`]: HTTP client for `/__ursula/metrics` and the admin endpoints.
//! - [`observe`]: read-only status and cluster-wide readiness reporting.
//! - [`plan`]: pure drain planning and per-node readiness checks.
//! - [`maintenance`]: node-maintenance verbs (drain, undrain, catch-up wait, rejoin arming).
//! - [`orchestrate`]: bare-metal rolling restart composing logical verbs with
//!   a physical restart command.
//! - [`operation`]: transports (providers) that reach each node's
//!   loopback-bound admin plane and, for bare metal, restart it.
//! - [`provider`]: cluster manifest loading and node addressing.

pub mod maintenance;
pub mod metrics;
pub mod observe;
pub mod operation;
pub mod orchestrate;
pub mod plan;
pub mod provider;

pub use maintenance::CatchUpOptions;
pub use maintenance::CatchUpOutcome;
pub use maintenance::DrainOptions;
pub use maintenance::DrainOutcome;
pub use maintenance::arm_empty_rejoin;
pub use maintenance::drain_node;
pub use maintenance::resolve_empty_rejoin_policy;
pub use maintenance::undrain_node;
pub use maintenance::wait_cluster_ready;
pub use maintenance::wait_node_ready;
pub use metrics::ClusterSnapshot;
pub use metrics::MetricsClient;
pub use metrics::RaftGroupView;
pub use observe::StatusReport;
pub use observe::wait_ready;
pub use observe::write_status;
pub use operation::AdminAccess;
pub use operation::OperationProvider;
pub use operation::ProviderKind;
pub use orchestrate::RestartOptions;
pub use orchestrate::RestartOutcome;
pub use orchestrate::RestartReport;
pub use orchestrate::run_restart;
pub use plan::DrainPlan;
pub use plan::GroupTransfer;
pub use plan::ReadinessReport;
pub use provider::NodeInfo;
pub use provider::NodeProvider;
pub use provider::RawProvider;
pub use provider::StaticNodeProvider;
