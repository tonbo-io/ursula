//! Operational library behind the `ursulactl` CLI.
//!
//! ursulactl manages logical cluster state over Ursula's admin and metrics
//! HTTP APIs: leadership placement, readiness, recovery and backups. It
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
//! - [`view`]: the meta leader's placement view and each group checked on
//!   its target voters, which `wait`, `wait-ready` and the verifications
//!   read.
//! - [`placement`]: placement views, `placement status`, `move`, `cancel`
//!   and `set-move-limit`, and `node replace-disk`.
//! - [`plan`]: pure drain planning and per-node readiness checks.
//! - [`backup`]: verifiable cluster backup, verification, and restore.
//! - [`maintenance`]: node-maintenance verbs (drain, undrain, catch-up wait).
//! - [`provider`]: cluster manifest loading and node addressing.
//! - [`quorum`]: fresh fixed-prefix verification of every group on its target
//!   voters.
//! - [`recovery`]: recovery plans for a group stalled after an accepted loss.

pub mod backup;
pub mod maintenance;
pub mod metrics;
pub mod observe;
pub mod placement;
pub mod plan;
pub mod provider;
pub mod quorum;
pub mod recovery;
pub mod view;

pub use maintenance::CatchUpOptions;
pub use maintenance::CatchUpOutcome;
pub use maintenance::DrainOptions;
pub use maintenance::DrainOutcome;
pub use maintenance::drain_node;
pub use maintenance::undrain_node;
pub use maintenance::wait_cluster_ready;
pub use maintenance::wait_node_ready;
pub use metrics::ClusterSnapshot;
pub use metrics::MetricsClient;
pub use metrics::RaftGroupView;
pub use observe::StatusReport;
pub use observe::wait_ready;
pub use observe::write_status;
pub use plan::DrainPlan;
pub use plan::GroupTransfer;
pub use plan::ReadinessReport;
pub use provider::NodeInfo;
pub use provider::NodeProvider;
pub use provider::StaticNodeProvider;
