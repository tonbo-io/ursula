//! Open committed local replicas and release removed ones independently of
//! the original startup placement configuration.

use tokio::sync::oneshot;
use tokio::sync::watch;
use ursula_control::ControlPlaneState;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::ShardRuntime;
use ursula_shard::RaftGroupId;
use ursula_shard::StaticShardMap;

/// The owning executor cancels and joins this service before group shutdown.
pub(crate) fn spawn_live_topology_cleanup(
    runtime: ShardRuntime,
    registry: RaftGroupHandleRegistry,
    node_id: u64,
    shard_map: StaticShardMap,
    mut topology: watch::Receiver<ControlPlaneState>,
) -> Result<oneshot::Receiver<()>, ursula_runtime::RuntimeError> {
    registry.set_control_topology(topology.clone());
    let owner = runtime.clone();
    owner.spawn_on_owner(ursula_shard::CoreId(0), async move {
        loop {
            let committed_groups: Vec<_> = topology
                .borrow()
                .placements
                .values()
                .filter(|placement| placement.hosts(node_id))
                .map(|placement| placement.raft_group_id)
                .collect();
            for group in committed_groups {
                if let Err(error) = runtime.warm_group(group).await {
                    tracing::error!(?group, %error, "failed to open committed local replica");
                }
            }
            let groups = registry.metrics_snapshot();
            for metrics in groups {
                let group = RaftGroupId(metrics.raft_group_id);
                if registry.control_hosts_group(group, node_id) != Some(false) {
                    continue;
                }
                let Some(placement) = shard_map.placement(group) else {
                    continue;
                };
                match runtime.shutdown_group_engine(placement).await {
                    Ok(()) => registry.forget_unhosted_group(group, node_id),
                    Err(error) => tracing::error!(?group, %error, "failed to stop removed replica"),
                }
            }
            if topology.changed().await.is_err() {
                return;
            }
        }
    })
}
