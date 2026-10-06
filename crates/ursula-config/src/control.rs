//! Opt-in managed control bootstrap, separate from static/dev configuration.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Component;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use ursula_control::ClusterBootstrap;
use ursula_control::ClusterId;
use ursula_control::ClusterIdentity;
use ursula_control::MetaLocalIdentity;
use ursula_control::NodeRegistration;
use ursula_control::PlacementPolicy;
use ursula_control::RoutingHashVersion;
use ursula_shard::RaftGroupId;

use crate::HumanDuration;
use crate::UrsulaConfig;
use crate::WalBackend;

/// Managed mode initially adopts a durable, already initialized static data
/// cluster. All data membership initialization flags must be disabled. Bootstrap
/// nodes/voters remain the immutable initial recipe after later scaling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    pub cluster_id: ClusterId,
    pub meta_journal_path: PathBuf,
    pub bootstrap_node_id: u64,
    #[serde(default)]
    pub initialize_meta_membership: bool,
    pub initial_meta_voters: Vec<u64>,
    pub node: NodeRegistration,
    pub bootstrap_nodes: Vec<NodeRegistration>,
    #[serde(default)]
    pub placement: PlacementPolicy,
    #[serde(default = "meta_snapshot_logs")]
    pub meta_snapshot_logs_since_last: u64,
    #[serde(default = "bootstrap_timeout")]
    pub bootstrap_timeout: HumanDuration,
    #[serde(default = "refresh_interval")]
    pub refresh_interval: HumanDuration,
}

fn meta_snapshot_logs() -> u64 {
    1000
}
fn bootstrap_timeout() -> HumanDuration {
    HumanDuration::sec(120)
}
fn refresh_interval() -> HumanDuration {
    HumanDuration::sec(1)
}

impl ControlConfig {
    pub fn local_identity(&self, config: &UrsulaConfig) -> Result<MetaLocalIdentity, String> {
        MetaLocalIdentity {
            cluster: ClusterIdentity {
                cluster_id: self.cluster_id.clone(),
                group_count: u32::try_from(config.raft.group_count)
                    .map_err(|_| "managed group_count exceeds u32".to_owned())?,
                core_count: u16::try_from(config.runtime.core_count)
                    .map_err(|_| "managed core_count exceeds u16".to_owned())?,
                routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
            },
            node: self.node.clone(),
        }
        .normalize()
    }

    pub fn bootstrap(&self, config: &UrsulaConfig) -> Result<ClusterBootstrap, String> {
        let local = self.local_identity(config)?;
        if local.node.node_id != config.raft.node_id {
            return Err("control.node.node_id must equal raft.node_id".to_owned());
        }
        if config.raft.wal.backend != WalBackend::Disk {
            return Err("managed control requires persistent data WAL".to_owned());
        }
        if config.raft.init_membership || config.raft.init_membership_per_group {
            return Err("managed control adopts existing data groups; disable both data membership initialization flags".to_owned());
        }
        let data_path = config
            .raft
            .wal
            .resolved_path()
            .ok_or_else(|| "managed data WAL path required".to_owned())?;
        for (name, path) in [
            ("control.meta_journal_path", &self.meta_journal_path),
            ("raft.wal.path", &data_path),
        ] {
            if !path.is_absolute()
                || path
                    .components()
                    .any(|part| matches!(part, Component::ParentDir))
            {
                return Err(format!("{name} must be an absolute path without '..'"));
            }
        }
        if self.meta_journal_path.file_name().is_none()
            || self.meta_journal_path.starts_with(&data_path)
        {
            return Err("meta journal must be a file outside the data WAL directory".to_owned());
        }
        let cluster_listen =
            config.server.cluster_listen.as_ref().ok_or_else(|| {
                "managed mode requires a separate server.cluster_listen".to_owned()
            })?;
        let addresses = [
            &config.server.listen,
            cluster_listen,
            &config.server.admin_listen,
        ];
        let mut binds = BTreeSet::new();
        for address in addresses {
            let bind = address
                .parse::<std::net::SocketAddr>()
                .map_err(|error| format!("invalid managed listener: {error}"))?;
            if bind.port() == 0 || !binds.insert(bind) {
                return Err("managed listeners must have distinct non-zero addresses".to_owned());
            }
        }
        if self.meta_snapshot_logs_since_last == 0 {
            return Err("meta_snapshot_logs_since_last must be non-zero".to_owned());
        }
        if self.bootstrap_timeout.as_duration().is_zero()
            || self.refresh_interval.as_duration().is_zero()
        {
            return Err(
                "managed bootstrap_timeout and refresh_interval must be non-zero".to_owned(),
            );
        }
        let mut nodes = BTreeMap::new();
        for node in &self.bootstrap_nodes {
            let node = node.clone().normalize()?;
            if nodes.insert(node.node_id, node).is_some() {
                return Err("duplicate control.bootstrap_nodes node_id".to_owned());
            }
        }
        if nodes
            .get(&local.node.node_id)
            .is_some_and(|node| node != &local.node)
        {
            return Err("local control node differs from bootstrap directory".to_owned());
        }
        let initial_meta_voters = self
            .initial_meta_voters
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if initial_meta_voters.len() != self.initial_meta_voters.len() {
            return Err("duplicate initial_meta_voters".to_owned());
        }
        if !initial_meta_voters.contains(&self.bootstrap_node_id) {
            return Err("bootstrap_node_id must be an initial meta voter".to_owned());
        }
        let mut voters = BTreeMap::new();
        for group in &config.raft.groups {
            let ids = group.voters.iter().copied().collect::<BTreeSet<_>>();
            if ids.len() != group.voters.len()
                || voters
                    .insert(RaftGroupId(group.raft_group_id), ids)
                    .is_some()
            {
                return Err("duplicate bootstrap data group/voter".to_owned());
            }
        }
        let recipe = ClusterBootstrap {
            identity: local.cluster,
            initial_meta_voters,
            nodes,
            voters,
            placement: self.placement.clone(),
        }
        .normalize()?;
        // Data replication endpoints and bootstrap discovery have one trusted
        // identity. Public redirects will use client_url separately.
        let expected = recipe
            .nodes
            .iter()
            .map(|(id, node)| (*id, node.cluster_url.clone()))
            .chain(std::iter::once((
                local.node.node_id,
                local.node.cluster_url.clone(),
            )))
            .collect::<BTreeMap<_, _>>();
        let mut peers = BTreeMap::new();
        for peer in &config.raft.peers {
            let normalized = NodeRegistration {
                node_id: peer.node_id,
                client_url: peer.url.clone(),
                cluster_url: peer.url.clone(),
                admin_url: peer.url.clone(),
                labels: BTreeMap::new(),
            }
            .normalize()?;
            if peers.insert(peer.node_id, normalized.cluster_url).is_some() {
                return Err("duplicate managed raft peer".to_owned());
            }
        }
        if peers != expected {
            return Err(
                "raft.peers must match the trusted bootstrap cluster origins and local node"
                    .to_owned(),
            );
        }
        if expected.values().any(|url| !url.starts_with("http://")) {
            return Err("managed cluster RPC requires HTTP origins on the private cluster plane; TLS transport is not configured".to_owned());
        }
        Ok(recipe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn fixture() -> UrsulaConfig {
        let nodes = (1..=5)
            .map(|id| NodeRegistration {
                node_id: id,
                client_url: format!("http://node{id}:4437"),
                cluster_url: format!("http://node{id}:4440"),
                admin_url: format!("http://node{id}:4438"),
                labels: BTreeMap::from([("zone".to_owned(), ((id - 1) % 3).to_string())]),
            })
            .collect::<Vec<_>>();
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.runtime.core_count = 1;
        config.raft.group_count = 2;
        config.server.cluster_listen = Some("127.0.0.1:4440".to_owned());
        config.raft.wal.backend = WalBackend::Disk;
        config.raft.wal.path = Some(PathBuf::from("/var/lib/ursula/data"));
        config.raft.peers = nodes
            .iter()
            .map(|node| crate::RaftPeerConfig {
                node_id: node.node_id,
                url: node.cluster_url.clone(),
            })
            .collect();
        config.raft.groups = vec![
            crate::RaftGroupConfig {
                raft_group_id: 0,
                voters: vec![1, 2, 3],
            },
            crate::RaftGroupConfig {
                raft_group_id: 1,
                voters: vec![1, 2, 3, 4, 5],
            },
        ];
        config.control = Some(ControlConfig {
            cluster_id: ClusterId::try_from("configuration-test".to_owned()).unwrap(),
            meta_journal_path: PathBuf::from("/var/lib/ursula/meta/meta.wal"),
            bootstrap_node_id: 1,
            initialize_meta_membership: false,
            initial_meta_voters: vec![1, 2, 3],
            node: nodes[0].clone(),
            bootstrap_nodes: nodes,
            placement: PlacementPolicy {
                group_overrides: vec![ursula_control::GroupPolicyOverride {
                    raft_group_id: RaftGroupId(1),
                    replication_factor: ursula_control::ReplicationFactor::Five,
                }],
                ..Default::default()
            },
            meta_snapshot_logs_since_last: 1000,
            bootstrap_timeout: bootstrap_timeout(),
            refresh_interval: refresh_interval(),
        });
        config
    }

    #[test]
    fn managed_configuration_round_trips_independent_meta_and_mixed_rf() {
        for count in [3, 5] {
            let mut config = fixture();
            config.control.as_mut().unwrap().initial_meta_voters = (1..=count).collect();
            config.validate().unwrap();
            let recipe = config.control.as_ref().unwrap().bootstrap(&config).unwrap();
            let text = toml::to_string_pretty(&config).unwrap();
            let restored: UrsulaConfig = toml::from_str(&text).unwrap();
            restored.validate().unwrap();
            assert_eq!(
                restored
                    .control
                    .as_ref()
                    .unwrap()
                    .bootstrap(&restored)
                    .unwrap(),
                recipe
            );
            assert_eq!(recipe.initial_meta_voters.len(), count as usize);
            assert_eq!(recipe.voters[&RaftGroupId(0)].len(), 3);
            assert_eq!(recipe.voters[&RaftGroupId(1)].len(), 5);
        }
        let mut five = fixture();
        five.control.as_mut().unwrap().placement = PlacementPolicy {
            default_replication_factor: ursula_control::ReplicationFactor::Five,
            group_overrides: vec![ursula_control::GroupPolicyOverride {
                raft_group_id: RaftGroupId(0),
                replication_factor: ursula_control::ReplicationFactor::Three,
            }],
            ..Default::default()
        };
        five.validate().unwrap();
    }

    #[test]
    fn managed_configuration_rejects_volatile_reinitializing_or_ambiguous_bootstrap() {
        type ConfigEdit = Box<dyn Fn(&mut UrsulaConfig)>;
        let edits: Vec<ConfigEdit> = vec![
            Box::new(|c| c.raft.wal.backend = WalBackend::Memory),
            Box::new(|c| c.raft.init_membership = true),
            Box::new(|c| c.raft.init_membership_per_group = true),
            Box::new(|c| c.raft.groups.clear()),
            Box::new(|c| c.server.cluster_listen = None),
            Box::new(|c| c.server.cluster_listen = Some(c.server.listen.clone())),
            Box::new(|c| c.control.as_mut().unwrap().initial_meta_voters = vec![1, 2, 3, 4]),
            Box::new(|c| c.control.as_mut().unwrap().initial_meta_voters = vec![1, 2, 3, 3]),
            Box::new(|c| c.control.as_mut().unwrap().bootstrap_node_id = 4),
            Box::new(|c| c.control.as_mut().unwrap().node.node_id = 2),
            Box::new(|c| {
                c.control.as_mut().unwrap().bootstrap_nodes[0].client_url =
                    "http://different:4437".to_owned()
            }),
            Box::new(|c| {
                c.control.as_mut().unwrap().bootstrap_nodes[4].cluster_url =
                    "http://node5:4440/path".to_owned()
            }),
            Box::new(|c| {
                c.control.as_mut().unwrap().meta_journal_path =
                    PathBuf::from("/var/lib/ursula/data/raft-log/meta.wal")
            }),
            Box::new(|c| {
                c.control.as_mut().unwrap().meta_journal_path = PathBuf::from("relative/meta.wal")
            }),
            Box::new(|c| c.raft.peers[0].url = "http://wrong:4440".to_owned()),
            Box::new(|c| c.control.as_mut().unwrap().bootstrap_timeout = HumanDuration::sec(0)),
            Box::new(|c| c.control.as_mut().unwrap().meta_snapshot_logs_since_last = 0),
        ];
        for (index, edit) in edits.into_iter().enumerate() {
            let mut config = fixture();
            edit(&mut config);
            assert!(config.validate().is_err(), "invalid case {index}");
        }
        let text = toml::to_string_pretty(&fixture()).unwrap();
        assert!(
            toml::from_str::<UrsulaConfig>(&text.replace(
                "default_replication_factor = 3",
                "default_replication_factor = 4"
            ))
            .is_err()
        );
    }
}
