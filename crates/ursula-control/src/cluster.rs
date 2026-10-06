//! Immutable cluster/routing identity and trusted bootstrap node inventory.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::model::NodeId;
use crate::policy::PlacementPolicy;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ClusterId(String);

impl TryFrom<String> for ClusterId {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(
                "cluster_id must be 1..=128 ASCII letters, digits, '-', '_' or '.'".to_owned(),
            );
        }
        Ok(Self(value))
    }
}

impl From<ClusterId> for String {
    fn from(value: ClusterId) -> Self {
        value.0
    }
}

impl ClusterId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Identifies the exact existing `StaticShardMap` routing algorithm, including
/// its bucket/stream separator. Unknown versions are rejected by serde.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutingHashVersion {
    Fnv1a64BucketSlashStreamV1,
}

impl RoutingHashVersion {
    pub fn wire_version(self) -> u32 {
        match self {
            Self::Fnv1a64BucketSlashStreamV1 => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterIdentity {
    pub cluster_id: ClusterId,
    pub group_count: u32,
    pub core_count: u16,
    pub routing_hash: RoutingHashVersion,
}

impl ClusterIdentity {
    pub fn validate(&self) -> Result<(), String> {
        if self.group_count == 0 || self.core_count == 0 {
            return Err("managed group_count and core_count must be non-zero".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeRegistration {
    pub node_id: NodeId,
    pub client_url: String,
    pub cluster_url: String,
    pub admin_url: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl NodeRegistration {
    pub fn validate(&self) -> Result<(), String> {
        if self.node_id == 0 {
            return Err("managed node_id must be non-zero".to_owned());
        }
        if self.labels.len() > 64
            || self.labels.iter().any(|(key, value)| {
                key.is_empty() || key.trim() != key || key.len() > 128 || value.len() > 1024
            })
        {
            return Err("managed nodes allow at most 64 labels, with non-empty unpadded keys up to 128 bytes and values up to 1024 bytes".to_owned());
        }
        for (role, value) in [
            ("client", &self.client_url),
            ("cluster", &self.cluster_url),
            ("admin", &self.admin_url),
        ] {
            if value.len() > 2048 {
                return Err(format!(
                    "node {} {role}_url exceeds 2048 bytes",
                    self.node_id
                ));
            }
            let url = url::Url::parse(value)
                .map_err(|error| format!("node {} invalid {role}_url: {error}", self.node_id))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.path() != "/"
                || value.trim() != value
            {
                return Err(format!(
                    "node {} {role}_url must be an HTTP(S) origin without credentials, path, query or fragment",
                    self.node_id
                ));
            }
        }
        Ok(())
    }

    pub fn normalize(mut self) -> Result<Self, String> {
        self.validate()?;
        for value in [
            &mut self.client_url,
            &mut self.cluster_url,
            &mut self.admin_url,
        ] {
            *value = url::Url::parse(value)
                .map_err(|error| error.to_string())?
                .to_string()
                .trim_end_matches('/')
                .to_owned();
        }
        Ok(self)
    }
}

/// The recipe is immutable after initial adoption. Live nodes/placements may
/// change later, while repeating the same recipe never resets their state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterBootstrap {
    pub identity: ClusterIdentity,
    pub initial_meta_voters: BTreeSet<NodeId>,
    pub nodes: BTreeMap<NodeId, NodeRegistration>,
    pub voters: BTreeMap<RaftGroupId, BTreeSet<NodeId>>,
    pub placement: PlacementPolicy,
}

/// Committed data-membership evidence collected using the data group's quorum
/// read barrier. The control state validates shape and policy; the server is
/// responsible for performing that barrier before submitting bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipLogId {
    pub term: u64,
    pub node_id: NodeId,
    pub index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedGroupMembership {
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub log_id: MembershipLogId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBootstrapRecord {
    pub recipe: ClusterBootstrap,
    pub memberships: BTreeMap<RaftGroupId, VerifiedGroupMembership>,
}

impl ClusterBootstrap {
    pub fn normalize(mut self) -> Result<Self, String> {
        self.identity.validate()?;
        self.placement.validate(self.identity.group_count)?;
        self.placement
            .group_overrides
            .sort_by_key(|entry| entry.raft_group_id);
        for (id, registration) in &mut self.nodes {
            if *id != registration.node_id {
                return Err("node directory key differs from registered node_id".to_owned());
            }
            *registration = registration.clone().normalize()?;
        }
        // A transport endpoint must address exactly one replica. Client and
        // admin origins can be the same as that node's cluster origin when the
        // operator deliberately shares listeners, but never another node's.
        let mut origins = BTreeMap::new();
        for node in self.nodes.values() {
            for origin in [&node.client_url, &node.cluster_url, &node.admin_url] {
                if let Some(owner) = origins.insert(origin, node.node_id)
                    && owner != node.node_id
                {
                    return Err(format!(
                        "endpoint {origin} is shared by nodes {owner} and {}",
                        node.node_id
                    ));
                }
            }
        }
        Ok(self)
    }
}

/// Durable local storage binding. This is installed before OpenRaft starts,
/// including before an uninitialized node can receive its first vote/snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaLocalIdentity {
    pub cluster: ClusterIdentity,
    pub node: NodeRegistration,
}

impl MetaLocalIdentity {
    pub fn normalize(mut self) -> Result<Self, String> {
        self.cluster.validate()?;
        self.node = self.node.normalize()?;
        Ok(self)
    }
}
