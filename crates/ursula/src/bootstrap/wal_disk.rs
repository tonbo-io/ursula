//! Raft WAL free-space gate.

use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_shard::RaftGroupId;

use crate::bootstrap::util::leader_counts;
use crate::bootstrap::util::prioritized_transfer_targets;
use crate::wal_disk::WalDiskMonitor;
use crate::wal_disk::WalDiskTransition;

const WAL_DISK_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) fn initialize_wal_disk_monitor(
    path: &Path,
    min_available_bytes: u64,
    resume_available_bytes: u64,
) -> io::Result<WalDiskMonitor> {
    fs::create_dir_all(path)?;
    let monitor = WalDiskMonitor::new(min_available_bytes, resume_available_bytes);
    match fs4::available_space(path) {
        Ok(available) => {
            let _ = monitor.observe_available(available);
        }
        Err(err) => {
            let _ = monitor.observe_error();
            tracing::error!(path = %path.display(), %err, "cannot inspect Raft WAL free space; failing readiness closed");
        }
    }
    Ok(monitor)
}

pub(crate) fn spawn_wal_disk_gate(
    path: PathBuf,
    monitor: WalDiskMonitor,
    registry: Option<RaftGroupHandleRegistry>,
    node_id: u64,
) {
    if !monitor.enabled() {
        return;
    }
    tokio::spawn(async move {
        if monitor.is_pressured() {
            enter_pressure(
                registry.as_ref(),
                node_id,
                monitor.snapshot().available_bytes,
            )
            .await;
        }
        loop {
            tokio::time::sleep(WAL_DISK_SAMPLE_INTERVAL).await;
            let transition = match fs4::available_space(&path) {
                Ok(available) => monitor.observe_available(available),
                Err(err) => {
                    tracing::error!(path = %path.display(), %err, "cannot inspect Raft WAL free space");
                    monitor.observe_error()
                }
            };
            match transition {
                WalDiskTransition::EnterPressure => {
                    enter_pressure(
                        registry.as_ref(),
                        node_id,
                        monitor.snapshot().available_bytes,
                    )
                    .await;
                }
                WalDiskTransition::LeavePressure => {
                    leave_pressure(
                        registry.as_ref(),
                        node_id,
                        monitor.snapshot().available_bytes,
                    );
                }
                WalDiskTransition::NoChange => {}
            }
        }
    });
}

async fn enter_pressure(
    registry: Option<&RaftGroupHandleRegistry>,
    node_id: u64,
    available_bytes: u64,
) {
    tracing::error!(
        node_id,
        available_bytes,
        "Raft WAL disk pressure entered; rejecting writes and yielding leadership"
    );
    let Some(registry) = registry else {
        return;
    };
    registry.mark_leadership_shed(LeadershipShedReason::WalDiskPressure);
    let snapshots = registry.metrics_snapshot();
    let counts = leader_counts(&snapshots);
    for snapshot in snapshots {
        let Some(raft) = registry.get(RaftGroupId(snapshot.raft_group_id)) else {
            continue;
        };
        if snapshot.current_leader != Some(node_id) {
            continue;
        }
        for target in prioritized_transfer_targets(&snapshot, node_id, &counts) {
            match raft.trigger().transfer_leader(target).await {
                Ok(()) => {
                    tracing::warn!(
                        node_id,
                        raft_group_id = snapshot.raft_group_id,
                        target,
                        "yielded leadership under Raft WAL disk pressure"
                    );
                    break;
                }
                Err(err) => tracing::error!(
                    raft_group_id = snapshot.raft_group_id,
                    target,
                    %err,
                    "failed to transfer leadership under Raft WAL disk pressure"
                ),
            }
        }
    }
}

fn leave_pressure(registry: Option<&RaftGroupHandleRegistry>, node_id: u64, available_bytes: u64) {
    tracing::warn!(
        node_id,
        available_bytes,
        "Raft WAL disk pressure cleared; accepting writes"
    );
    let Some(registry) = registry else {
        return;
    };
    registry.clear_leadership_shed(LeadershipShedReason::WalDiskPressure);
}
