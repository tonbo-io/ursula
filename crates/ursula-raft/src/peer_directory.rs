//! Where each configured peer listens (`raft.peers`), built once per process.
//!
//! [`PeerDirectory`] maps a node id to its [`PeerEndpoints`]: the URL peers
//! dial for its cluster plane and the URL clients reach its client plane at.
//! Every reader of a configured peer address takes it from here: the engine
//! factory and its [`crate::PeerCluster`], the leader router, the
//! leadership-shed polls, the egress probe, the shutdown handoff and the
//! format-epoch probe. URLs are normalised once, without a trailing `/`, so
//! a caller appends a path to them as they are.

use std::collections::BTreeMap;
use std::sync::Arc;

/// The URLs one peer listens at, without a trailing `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEndpoints {
    /// The URL peers dial for the cluster plane (`url`).
    cluster: String,
    /// The URL clients reach the client plane at (`client_url`, else `url`).
    client: String,
}

impl PeerEndpoints {
    /// A peer that serves its cluster plane at `cluster` and its client plane
    /// at `client`.
    pub fn new(cluster: impl Into<String>, client: impl Into<String>) -> Self {
        Self {
            cluster: normalise(cluster.into()),
            client: normalise(client.into()),
        }
    }

    /// A peer that serves both planes at `url` (`server.listen`).
    pub fn shared(url: impl Into<String>) -> Self {
        let url = normalise(url.into());
        Self {
            client: url.clone(),
            cluster: url,
        }
    }

    pub fn cluster_url(&self) -> &str {
        &self.cluster
    }

    pub fn client_url(&self) -> &str {
        &self.client
    }
}

impl From<&ursula_config::RaftPeerConfig> for PeerEndpoints {
    fn from(peer: &ursula_config::RaftPeerConfig) -> Self {
        Self::new(peer.url.as_str(), peer.client_base_url())
    }
}

fn normalise(mut url: String) -> String {
    let trimmed = url.trim_end_matches('/').len();
    url.truncate(trimmed);
    url
}

/// `raft.peers`: node id to the endpoints that node listens at. It lists
/// this node too. Cloning shares one map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerDirectory(Arc<BTreeMap<u64, PeerEndpoints>>);

impl PeerDirectory {
    /// The directory of a node's `raft.peers`.
    pub fn from_config(peers: &[ursula_config::RaftPeerConfig]) -> Self {
        Self::new(
            peers
                .iter()
                .map(|peer| (peer.node_id, PeerEndpoints::from(peer))),
        )
    }

    pub fn new(peers: impl IntoIterator<Item = (u64, PeerEndpoints)>) -> Self {
        Self(Arc::new(peers.into_iter().collect()))
    }

    /// The URL peers dial for `node_id`'s cluster plane.
    pub fn cluster_url(&self, node_id: u64) -> Result<&str, PeerUnknown> {
        self.0
            .get(&node_id)
            .map(PeerEndpoints::cluster_url)
            .ok_or(PeerUnknown { node_id })
    }

    /// The URL clients reach `node_id`'s client plane at.
    pub fn client_url(&self, node_id: u64) -> Option<&str> {
        self.0.get(&node_id).map(PeerEndpoints::client_url)
    }

    /// Whether `raft.peers` lists `node_id`.
    pub fn contains(&self, node_id: u64) -> bool {
        self.0.contains_key(&node_id)
    }

    /// Every listed node id, ascending.
    pub fn node_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.0.keys().copied()
    }

    /// Every listed node with its endpoints, by ascending node id.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &PeerEndpoints)> + '_ {
        self.0
            .iter()
            .map(|(node_id, endpoints)| (*node_id, endpoints))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// `raft.peers` does not list the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("raft.peers lists no node {node_id}")]
pub struct PeerUnknown {
    pub node_id: u64,
}

#[cfg(test)]
mod tests {
    use ursula_config::RaftPeerConfig;

    use super::PeerDirectory;
    use super::PeerEndpoints;
    use super::PeerUnknown;

    fn peer(node_id: u64, url: &str, client_url: Option<&str>) -> RaftPeerConfig {
        RaftPeerConfig {
            node_id,
            url: url.to_owned(),
            client_url: client_url.map(str::to_owned),
        }
    }

    #[test]
    fn the_client_url_defaults_to_the_cluster_url() {
        let directory = PeerDirectory::from_config(&[
            peer(1, "http://node-1:4437", None),
            peer(2, "http://node-2:4438", Some("http://node-2:4437")),
        ]);
        assert_eq!(directory.cluster_url(1), Ok("http://node-1:4437"));
        assert_eq!(directory.client_url(1), Some("http://node-1:4437"));
        assert_eq!(directory.cluster_url(2), Ok("http://node-2:4438"));
        assert_eq!(directory.client_url(2), Some("http://node-2:4437"));
        assert_eq!(directory.node_ids().collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn trailing_slashes_are_trimmed_once() {
        let directory = PeerDirectory::from_config(&[peer(
            1,
            "http://node-1:4438/",
            Some("http://node-1:4437//"),
        )]);
        assert_eq!(directory.cluster_url(1), Ok("http://node-1:4438"));
        assert_eq!(directory.client_url(1), Some("http://node-1:4437"));
        assert_eq!(
            PeerEndpoints::shared("http://node-1:4437/"),
            PeerEndpoints::new("http://node-1:4437", "http://node-1:4437")
        );
    }

    #[test]
    fn an_unlisted_node_is_unknown() {
        let directory = PeerDirectory::new([(1, PeerEndpoints::shared("http://node-1"))]);
        assert_eq!(directory.cluster_url(2), Err(PeerUnknown { node_id: 2 }));
        assert_eq!(directory.client_url(2), None);
        assert!(!directory.contains(2));
        assert!(PeerDirectory::default().is_empty());
    }
}
