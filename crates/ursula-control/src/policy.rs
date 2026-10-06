//! Replication and failure-domain policy, independent of transport and scheduling.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::model::ClusterNode;
use crate::model::NodeId;

/// Supported steady-state voter counts in managed mode. Static/dev membership
/// remains independent of this policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub enum ReplicationFactor {
    #[default]
    Three,
    Five,
}

impl From<ReplicationFactor> for u32 {
    fn from(value: ReplicationFactor) -> Self {
        match value {
            ReplicationFactor::Three => 3,
            ReplicationFactor::Five => 5,
        }
    }
}

impl TryFrom<u32> for ReplicationFactor {
    type Error = String;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            3 => Ok(Self::Three),
            5 => Ok(Self::Five),
            _ => Err(format!(
                "managed replication factor must be 3 or 5, got {value}"
            )),
        }
    }
}

impl ReplicationFactor {
    pub fn voter_count(self) -> usize {
        match self {
            Self::Three => 3,
            Self::Five => 5,
        }
    }

    pub fn quorum(self) -> usize {
        self.voter_count() / 2 + 1
    }

    pub fn tolerated_failures(self) -> usize {
        self.voter_count() - self.quorum()
    }
}

/// The resolved policy is persisted per group; changing bootstrap defaults
/// never reinterprets an existing placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GroupPlacementPolicy {
    pub replication_factor: ReplicationFactor,
    pub failure_domain: String,
    pub survive_failure_domains: u32,
}

impl Default for GroupPlacementPolicy {
    fn default() -> Self {
        Self {
            replication_factor: ReplicationFactor::Three,
            failure_domain: "zone".to_owned(),
            survive_failure_domains: 1,
        }
    }
}

impl GroupPlacementPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.failure_domain.trim().is_empty()
            || self.failure_domain.trim() != self.failure_domain
        {
            return Err("failure_domain must be a non-empty, unpadded label key".to_owned());
        }
        if self.survive_failure_domains != 1 {
            return Err(
                "managed placement currently supports loss of exactly one failure domain"
                    .to_owned(),
            );
        }
        Ok(())
    }

    /// Validate a uniform voter configuration. During a joint transition both
    /// constituent configurations must pass independently, not their union.
    /// Node liveness/eligibility is checked by the caller so a failed source
    /// does not prevent planning its evacuation.
    pub fn validate_voters(
        &self,
        voters: &BTreeSet<NodeId>,
        nodes: &BTreeMap<NodeId, ClusterNode>,
    ) -> Result<(), String> {
        self.validate()?;
        if voters.len() != self.replication_factor.voter_count() {
            return Err(format!(
                "expected {} voters, found {}",
                self.replication_factor.voter_count(),
                voters.len()
            ));
        }
        let mut domains = BTreeMap::<&str, usize>::new();
        for id in voters {
            let node = nodes
                .get(id)
                .ok_or_else(|| format!("voter node {id} is not registered"))?;
            let domain = node
                .labels
                .get(&self.failure_domain)
                .filter(|value| !value.trim().is_empty() && value.trim() == *value)
                .ok_or_else(|| {
                    format!(
                        "voter node {id} lacks a valid '{}' label",
                        self.failure_domain
                    )
                })?;
            let count = domains.entry(domain.as_str()).or_default();
            *count += 1;
            if *count > self.replication_factor.tolerated_failures() {
                return Err(format!(
                    "failure domain '{domain}' exceeds {} voters allowed by one-domain-loss policy",
                    self.replication_factor.tolerated_failures()
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupPolicyOverride {
    pub raft_group_id: RaftGroupId,
    pub replication_factor: ReplicationFactor,
}

/// One-time bootstrap policy. The list representation also supports TOML's
/// `[[control.placement.group_overrides]]` syntax; duplicate IDs are rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlacementPolicy {
    pub default_replication_factor: ReplicationFactor,
    pub failure_domain: String,
    pub survive_failure_domains: u32,
    pub group_overrides: Vec<GroupPolicyOverride>,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        Self {
            default_replication_factor: ReplicationFactor::Three,
            failure_domain: "zone".to_owned(),
            survive_failure_domains: 1,
            group_overrides: Vec::new(),
        }
    }
}

impl PlacementPolicy {
    pub fn validate(&self, group_count: u32) -> Result<(), String> {
        if group_count == 0 {
            return Err("managed group_count must be non-zero".to_owned());
        }
        self.resolve(RaftGroupId(0)).validate()?;
        let mut ids = BTreeSet::new();
        for entry in &self.group_overrides {
            if entry.raft_group_id.0 >= group_count {
                return Err(format!(
                    "policy override group {} is outside group_count {group_count}",
                    entry.raft_group_id.0
                ));
            }
            if !ids.insert(entry.raft_group_id) {
                return Err(format!(
                    "duplicate policy override for group {}",
                    entry.raft_group_id.0
                ));
            }
        }
        Ok(())
    }

    pub fn resolve(&self, raft_group_id: RaftGroupId) -> GroupPlacementPolicy {
        GroupPlacementPolicy {
            replication_factor: self
                .group_overrides
                .iter()
                .find(|entry| entry.raft_group_id == raft_group_id)
                .map_or(self.default_replication_factor, |entry| {
                    entry.replication_factor
                }),
            failure_domain: self.failure_domain.clone(),
            survive_failure_domains: self.survive_failure_domains,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPlacement {
    pub group_count: u32,
    pub bootstrap_policy: PlacementPolicy,
    pub groups: BTreeMap<RaftGroupId, GroupPlacementPolicy>,
}
