use std::collections::BTreeSet;

use thiserror::Error;

use crate::config::ColdBackend;
use crate::config::UrsulaConfig;
use crate::config::WalBackend;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("raft.wal.path is required when backend is 'disk'")]
    RaftWalPathRequired,
    #[error("storage.cold.s3.bucket is required when cold backend is 's3'")]
    ColdS3BucketRequired,
    #[error("raft.node_id {0} must be present in raft.peers")]
    NodeIdNotInPeers(u64),
    #[error("raft group {0} has no voters")]
    EmptyVoters(u32),
    #[error("raft group {0} voter {1} is not present in raft.peers")]
    VoterNotInPeers(u32, u64),
    #[error("raft group {0} is outside configured raft.group_count {1}")]
    GroupOutOfRange(u32, usize),
    #[error("partial raft.groups config is not supported; missing raft group {0} of {1}")]
    MissingGroup(u32, usize),
    #[error("{0}")]
    Other(String),
}

impl UrsulaConfig {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.raft.node_id == 0 {
            return Err(ValidationError::Other(
                "raft.node_id is required (use --node-id CLI flag)".into(),
            ));
        }
        if self.raft.wal.backend == WalBackend::Disk && self.raft.wal.path.is_none() {
            return Err(ValidationError::RaftWalPathRequired);
        }
        if self.storage.cold.backend == ColdBackend::S3 {
            let bucket = self
                .storage
                .cold
                .s3
                .as_ref()
                .and_then(|s3| s3.bucket.as_ref());
            if bucket.is_none() || bucket.unwrap().trim().is_empty() {
                return Err(ValidationError::ColdS3BucketRequired);
            }
        }
        self.validate_peers()?;
        if !self.raft.groups.is_empty() {
            let peer_ids: BTreeSet<u64> = self.raft.peers.iter().map(|p| p.node_id).collect();
            self.validate_groups(&peer_ids)?;
        }
        self.validate_non_zero_durations()?;
        if self.raft.snapshot_pressure_unpurged_logs == 0 {
            return Err(ValidationError::Other(
                "raft.snapshot_pressure_unpurged_logs must be non-zero".into(),
            ));
        }
        if self.raft.snapshot_pressure_max_groups_per_tick == 0 {
            return Err(ValidationError::Other(
                "raft.snapshot_pressure_max_groups_per_tick must be non-zero".into(),
            ));
        }
        if self.storage.cold.compaction_target_size.as_bytes()
            > self.storage.cold.compaction_max_size.as_bytes()
        {
            return Err(ValidationError::Other(
                "storage.cold.compaction_target_size must not exceed compaction_max_size".into(),
            ));
        }
        self.validate_cold_health_watermarks()?;
        Ok(())
    }

    fn validate_cold_health_watermarks(&self) -> Result<(), ValidationError> {
        let low = self.governance.cold_health.hot_size_low.as_bytes();
        let high = self.governance.cold_health.hot_size_high.as_bytes();
        let flush_min = self.storage.cold.flush_min_hot_size().as_bytes();
        if low >= high {
            return Err(ValidationError::Other(
                "governance.cold_health.hot_size_low must be lower than hot_size_high".into(),
            ));
        }
        if high <= flush_min {
            return Err(ValidationError::Other(format!(
                "governance.cold_health.hot_size_high ({high} bytes) must exceed storage.cold.flush_min_hot_size ({flush_min} bytes) so normal flushing can start before leadership shedding",
            )));
        }
        if let Some(max_hot) = self.storage.cold.max_hot_size_per_group
            && max_hot.as_bytes() > 0
            && high >= max_hot.as_bytes()
        {
            return Err(ValidationError::Other(format!(
                "governance.cold_health.hot_size_high ({high} bytes) must be lower than storage.cold.max_hot_size_per_group ({} bytes)",
                max_hot.as_bytes(),
            )));
        }
        Ok(())
    }

    fn validate_peers(&self) -> Result<(), ValidationError> {
        let mut seen_ids = BTreeSet::new();
        for peer in &self.raft.peers {
            if !seen_ids.insert(peer.node_id) {
                return Err(ValidationError::Other(format!(
                    "duplicate raft peer node_id {}",
                    peer.node_id,
                )));
            }
        }
        if !self.raft.peers.is_empty() && !seen_ids.contains(&self.raft.node_id) {
            return Err(ValidationError::NodeIdNotInPeers(self.raft.node_id));
        }
        Ok(())
    }

    fn validate_non_zero_durations(&self) -> Result<(), ValidationError> {
        for (name, value) in [
            ("raft.rejoin_probe", self.raft.rejoin_probe.as_duration()),
            (
                "raft.bootstrap_peer_probe_interval",
                self.raft.bootstrap_peer_probe_interval.as_duration(),
            ),
            (
                "storage.cold.flush_interval",
                self.storage.cold.flush_interval.as_duration(),
            ),
            (
                "storage.cold.gc_interval",
                self.storage.cold.gc_interval.as_duration(),
            ),
            (
                "storage.cold.compaction_interval",
                self.storage.cold.compaction_interval.as_duration(),
            ),
            (
                "storage.cold.compaction_gc_grace",
                self.storage.cold.compaction_gc_grace.as_duration(),
            ),
        ] {
            if value.is_zero() {
                return Err(ValidationError::Other(format!("{name} must be non-zero",)));
            }
        }
        Ok(())
    }

    fn validate_groups(&self, peer_ids: &BTreeSet<u64>) -> Result<(), ValidationError> {
        let groups = &self.raft.groups;
        if groups.is_empty() {
            return Ok(());
        }
        if self.raft.peers.is_empty() {
            return Err(ValidationError::Other(
                "raft.groups requires at least one raft peer".into(),
            ));
        }

        let group_count = u32::try_from(self.raft.group_count).map_err(|_| {
            ValidationError::Other(format!(
                "raft.group_count {} exceeds u32::MAX",
                self.raft.group_count
            ))
        })?;

        let mut seen_group_ids = BTreeSet::new();
        for group in groups {
            if !seen_group_ids.insert(group.raft_group_id) {
                return Err(ValidationError::Other(format!(
                    "duplicate raft group_id {}",
                    group.raft_group_id,
                )));
            }
            if group.raft_group_id >= group_count {
                return Err(ValidationError::GroupOutOfRange(
                    group.raft_group_id,
                    self.raft.group_count,
                ));
            }
            if group.voters.is_empty() {
                return Err(ValidationError::EmptyVoters(group.raft_group_id));
            }
            for voter in &group.voters {
                if !peer_ids.contains(voter) {
                    return Err(ValidationError::VoterNotInPeers(
                        group.raft_group_id,
                        *voter,
                    ));
                }
            }
        }

        for raw_group_id in 0..group_count {
            if !groups.iter().any(|g| g.raft_group_id == raw_group_id) {
                return Err(ValidationError::MissingGroup(
                    raw_group_id,
                    self.raft.group_count,
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::UrsulaConfig;
    use crate::human::HumanSize;

    #[test]
    fn cold_health_watermarks_leave_room_between_flush_and_backpressure() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.storage.cold.flush_min_hot_size = Some(HumanSize::mib(8));
        config.storage.cold.max_hot_size_per_group = Some(HumanSize::mib(64));
        config.governance.cold_health.hot_size_low = HumanSize::mib(32);
        config.governance.cold_health.hot_size_high = HumanSize::mib(48);
        config.validate().expect("valid cold-health window");

        config.governance.cold_health.hot_size_low = HumanSize::mib(4);
        config.governance.cold_health.hot_size_high = HumanSize::mib(8);
        let error = config
            .validate()
            .expect_err("shedding at the flush threshold must be rejected");
        assert!(error.to_string().contains("normal flushing can start"));

        config.governance.cold_health.hot_size_low = HumanSize::mib(32);
        config.governance.cold_health.hot_size_high = HumanSize::mib(64);
        let error = config
            .validate()
            .expect_err("shedding at the backpressure cliff must be rejected");
        assert!(error.to_string().contains("max_hot_size_per_group"));
    }

    #[test]
    fn snapshot_pressure_limits_must_be_non_zero() {
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.raft.snapshot_pressure_unpurged_logs = 0;
        let error = config
            .validate()
            .expect_err("zero unpurged-log watermark must be rejected");
        assert!(
            error
                .to_string()
                .contains("snapshot_pressure_unpurged_logs")
        );

        config.raft.snapshot_pressure_unpurged_logs = 1;
        config.raft.snapshot_pressure_max_groups_per_tick = 0;
        let error = config
            .validate()
            .expect_err("zero pressure batch must be rejected");
        assert!(
            error
                .to_string()
                .contains("snapshot_pressure_max_groups_per_tick")
        );
    }
}
