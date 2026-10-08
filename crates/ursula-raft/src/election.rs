//! Local participation policy, independent of group registration and transport.
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ursula_shard::RaftGroupId;

use crate::UrsulaRaftTypeConfig;
use crate::owner::OwnerRaftHandle;
use crate::rejoin::GroupRejoin;

pub type LeadershipShedFlag = Arc<AtomicU8>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadershipShedReason {
    SnapshotDriverS3 = 0b001,
    ClusterEgress = 0b010,
    ColdHealth = 0b100,
    MaintenanceDrain = 0b1000,
    WalDiskPressure = 0b1_0000,
}

impl LeadershipShedReason {
    const ALL: [Self; 5] = [
        Self::SnapshotDriverS3,
        Self::ClusterEgress,
        Self::ColdHealth,
        Self::MaintenanceDrain,
        Self::WalDiskPressure,
    ];

    pub(crate) const fn bit(self) -> u8 {
        self as u8
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::SnapshotDriverS3 => "snapshot-driver-s3",
            Self::ClusterEgress => "cluster-egress",
            Self::ColdHealth => "cold-health",
            Self::MaintenanceDrain => "maintenance-drain",
            Self::WalDiskPressure => "wal-disk-pressure",
        }
    }
}

impl fmt::Display for LeadershipShedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct LeadershipShedState: u8 {
        const SNAPSHOT_DRIVER_S3 = LeadershipShedReason::SnapshotDriverS3.bit();
        const CLUSTER_EGRESS = LeadershipShedReason::ClusterEgress.bit();
        const COLD_HEALTH = LeadershipShedReason::ColdHealth.bit();
        const MAINTENANCE_DRAIN = LeadershipShedReason::MaintenanceDrain.bit();
        const WAL_DISK_PRESSURE = LeadershipShedReason::WalDiskPressure.bit();
    }
}

impl From<LeadershipShedReason> for LeadershipShedState {
    fn from(reason: LeadershipShedReason) -> Self {
        Self::from_bits_truncate(reason.bit())
    }
}

impl LeadershipShedState {
    pub fn load(flag: &LeadershipShedFlag) -> Self {
        Self::from_bits_truncate(flag.load(Ordering::Acquire))
    }

    pub fn is_shed(self) -> bool {
        !self.is_empty()
    }

    /// Transfers trigger an election, so they use the campaign policy too.
    #[cfg(test)]
    pub(crate) fn should_accept_transfer(self) -> bool {
        self.should_campaign()
    }

    /// Whether local raft groups should be allowed to campaign.
    ///
    /// Cluster-egress and local S3 snapshot-driver impairment disable
    /// campaigning. Cold-health is softer: the node should shed excess current
    /// leadership, but it must remain electable so a cluster-wide hot backlog
    /// cannot exclude every node from leadership.
    pub fn should_campaign(self) -> bool {
        !self.intersects(
            Self::CLUSTER_EGRESS
                | Self::SNAPSHOT_DRIVER_S3
                | Self::MAINTENANCE_DRAIN
                | Self::WAL_DISK_PRESSURE,
        )
    }

    /// Whether local raft groups should actively move current leadership away.
    pub fn should_shed_current_leaders(self) -> bool {
        self.is_shed()
    }

    #[cfg(test)]
    pub(crate) fn transfer_rejection_reason(self) -> Option<LeadershipShedReason> {
        if self.contains(Self::CLUSTER_EGRESS) {
            Some(LeadershipShedReason::ClusterEgress)
        } else if self.contains(Self::MAINTENANCE_DRAIN) {
            Some(LeadershipShedReason::MaintenanceDrain)
        } else if self.contains(Self::WAL_DISK_PRESSURE) {
            Some(LeadershipShedReason::WalDiskPressure)
        } else if self.contains(Self::SNAPSHOT_DRIVER_S3) {
            Some(LeadershipShedReason::SnapshotDriverS3)
        } else {
            None
        }
    }
}

impl fmt::Display for LeadershipShedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wrote = false;
        for reason in LeadershipShedReason::ALL {
            if self.contains(reason.into()) {
                if wrote {
                    f.write_str("|")?;
                }
                fmt::Display::fmt(&reason, f)?;
                wrote = true;
            }
        }
        if !wrote {
            f.write_str("none")?;
        }
        Ok(())
    }
}

/// Serializes policy observations with updates to OpenRaft's election switch.
#[derive(Debug, Clone, Default)]
pub struct ElectionPolicy {
    shed: LeadershipShedFlag,
}

impl ElectionPolicy {
    pub(crate) fn flag(&self) -> LeadershipShedFlag {
        self.shed.clone()
    }
    pub fn state(&self) -> LeadershipShedState {
        LeadershipShedState::load(&self.shed)
    }
    pub fn may_campaign(&self, gate: Option<&GroupRejoin>) -> bool {
        self.state().should_campaign() && gate.is_none_or(GroupRejoin::may_campaign)
    }
    /// The one handoff policy: whether `target` can take over this node's
    /// leadership of `group` now. OpenRaft parks a leader whose transfer
    /// cannot complete, so the target must be another voter that answered
    /// this leader within `ack_window` (`target_ack_age` is how long ago it
    /// last did), holds every committed entry, and did not lose its log.
    pub(crate) fn validate_handoff(
        &self,
        group: RaftGroupId,
        target: u64,
        metrics: &openraft::RaftMetrics<UrsulaRaftTypeConfig>,
        gate: Option<&GroupRejoin>,
        target_ack_age: Option<Duration>,
        ack_window: Duration,
    ) -> Result<(), LeadershipTransferError> {
        if metrics.current_leader != Some(metrics.id) {
            return Err(LeadershipTransferError::NotLeader { group });
        }
        if target == metrics.id || !metrics.membership_config.voter_ids().any(|id| id == target) {
            return Err(LeadershipTransferError::InvalidTarget { group, target });
        }
        if gate.is_some_and(|gate| gate.is_reverted_follower(target)) {
            return Err(LeadershipTransferError::RecoveringTarget { group, target });
        }
        if target_ack_age.is_none_or(|age| age > ack_window) {
            return Err(LeadershipTransferError::UnreachableTarget {
                group,
                target,
                ack_age: target_ack_age,
            });
        }
        let matched = metrics
            .replication
            .as_ref()
            .and_then(|replication| replication.get(&target))
            .and_then(|matched| matched.as_ref())
            .map(|log_id| log_id.index());
        let committed = metrics
            .local_committed
            .as_ref()
            .map(|log_id| log_id.index());
        if matched < committed {
            return Err(LeadershipTransferError::LaggingTarget {
                group,
                target,
                matched,
                committed,
            });
        }
        Ok(())
    }

    pub(crate) fn refresh(&self, raft: &OwnerRaftHandle, gate: Option<Arc<GroupRejoin>>) {
        let policy = self.clone();
        raft.elect(move || policy.may_campaign(gate.as_deref()));
    }
}

/// A rejected handoff never reaches OpenRaft's transfer state.
#[derive(Debug, thiserror::Error)]
pub enum LeadershipTransferError {
    #[error("Raft group {group:?} is not registered")]
    NotRegistered { group: RaftGroupId },
    #[error("node is not leader of Raft group {group:?}")]
    NotLeader { group: RaftGroupId },
    #[error("node {target} is not another voter of Raft group {group:?}")]
    InvalidTarget { group: RaftGroupId, target: u64 },
    #[error("node {target} lost its log in Raft group {group:?}")]
    RecoveringTarget { group: RaftGroupId, target: u64 },
    #[error(
        "node {target} has not answered the leader of Raft group {group:?} recently (last answer \
         {ack_age:?} ago)"
    )]
    UnreachableTarget {
        group: RaftGroupId,
        target: u64,
        ack_age: Option<Duration>,
    },
    #[error(
        "node {target} matched {matched:?} of Raft group {group:?}, short of the committed \
         {committed:?}"
    )]
    LaggingTarget {
        group: RaftGroupId,
        target: u64,
        matched: Option<u64>,
        committed: Option<u64>,
    },
    #[error("OpenRaft leadership transfer failed for {group:?}: {source}")]
    Raft {
        group: RaftGroupId,
        #[source]
        source: openraft::error::Fatal<UrsulaRaftTypeConfig>,
    },
}
