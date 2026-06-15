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
