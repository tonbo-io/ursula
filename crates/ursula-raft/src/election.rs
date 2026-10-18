//! Local participation policy, independent of group registration and transport.
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::raft::TransferLeaderRequest;
use openraft::vote::RaftLeaderId;
use ursula_runtime::GroupEngineError;
use ursula_shard::RaftGroupId;

use crate::UrsulaRaftTypeConfig;
use crate::UrsulaVote;
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

    /// The one policy for a leadership transfer this node receives, from a
    /// leader or from a self-election. A transfer to another node only
    /// drops this node's leader lease. A transfer to this node makes it
    /// campaign, so its campaign policy must allow that, and it must not
    /// carry this node's own committed vote.
    ///
    /// OpenRaft acts on a transfer only while this node's vote equals the
    /// vote the transfer carries, and compares the two in its core as it
    /// handles the transfer. A transfer to this node that carries its own
    /// committed vote can therefore act only while this node leads under
    /// that vote. OpenRaft would then start an election that keeps the old
    /// leader state, and a leader call before that election ends breaks
    /// its invariant that the leader's vote is at least the node's vote
    /// (an assertion that stops the core thread in debug builds).
    ///
    /// The vote rule reads the vote in the request rather than this node's
    /// state, so nothing can change between that check and the core. The
    /// campaign policy reads the recovery gate and the leadership shed,
    /// which can change before the core acts on the transfer (while the
    /// log flushes, up to `election_timeout_min`): a shed that starts
    /// meanwhile does not stop a transfer accepted before it. A transfer
    /// the vote rule accepts acts only while this node holds another node's
    /// vote or its own uncommitted vote. OpenRaft holds leader state under
    /// such a vote only after an election started on a leader. A transfer
    /// starts one only with the leader's committed vote, which is refused
    /// here. `trigger().elect()` on a leader starts one too, so every caller
    /// that may hit a leader goes through `owner::step_down_and_campaign`,
    /// which steps the leader down first. So would a vote request
    /// that names this node as its own candidate in a higher term: OpenRaft
    /// accepts it at once when it is marked as a transfer's, and otherwise
    /// once the leader's own vote lease (set once, when it became leader)
    /// has expired. No peer sends one, and the vote handler does not refuse
    /// it, whatever its transfer mark.
    pub(crate) fn validate_incoming_transfer(
        &self,
        group: RaftGroupId,
        node_id: u64,
        request: &TransferLeaderRequest<UrsulaRaftTypeConfig>,
        gate: Option<&GroupRejoin>,
    ) -> Result<(), IncomingTransferError> {
        if *request.to_node_id() != node_id {
            return Ok(());
        }
        let vote = *request.from_leader();
        if vote.is_committed() && *vote.leader_id().node_id() == node_id {
            return Err(IncomingTransferError::OwnCommittedVote { group, vote });
        }
        if !self.may_campaign(gate) {
            return Err(IncomingTransferError::MayNotCampaign { group });
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

/// A leadership transfer this node received and refused before OpenRaft saw
/// it, or could not hand to OpenRaft.
#[derive(Debug, thiserror::Error)]
pub enum IncomingTransferError {
    #[error(transparent)]
    Group(#[from] GroupEngineError),
    /// A transfer to this node that carries its own committed vote: it
    /// leads under that vote, or led and has seen no newer one.
    #[error(
        "a leadership transfer of Raft group {group:?} to this node carries its own committed \
         vote {vote} (it leads, or led and has seen no newer vote); refusing it"
    )]
    OwnCommittedVote {
        group: RaftGroupId,
        vote: UrsulaVote,
    },
    #[error(
        "node may not campaign for Raft group {group:?} (recovery gate closed or leadership \
         shed); refusing a leadership transfer to itself"
    )]
    MayNotCampaign { group: RaftGroupId },
    #[error("OpenRaft could not take the leadership transfer for {group:?}: {source}")]
    Raft {
        group: RaftGroupId,
        #[source]
        source: openraft::error::Fatal<UrsulaRaftTypeConfig>,
    },
}
