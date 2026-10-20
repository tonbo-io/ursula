//! The cluster-id check between peers on the Raft gRPC plane (#581 section
//! 6.5).
//!
//! A node's cluster id is bound to its WAL root. Every Raft gRPC request a
//! node sends carries that id and the sender's node id as request metadata
//! ([`CLUSTER_ID_METADATA`], [`NODE_ID_METADATA`]), stamped by
//! [`PeerIdentity`]. Every node checks them in a tonic interceptor
//! ([`ClusterCheck`]), which runs before the request is decoded and before
//! any Raft RPC is handled:
//!
//! - the same id: the RPC runs;
//! - another id: refused `FAILED_PRECONDITION`
//!   ([`PeerClusterError::Mismatch`]). The refusal carries this node's id in
//!   its [`CLUSTER_ID_METADATA`], so the sender learns it too, and both sides
//!   record the other;
//! - no id: a sender before format epoch 4 or a stray client, refused
//!   `FAILED_PRECONDITION` with the Raft protocol-mismatch text, which such a
//!   sender reads as a format-epoch refusal (E8). It is recorded like a
//!   mismatch and does not latch this node's format-epoch readiness: a node
//!   outside the cluster must not take a healthy node out of service.
//!
//! A refusal is permanent: only an operator who fixes `server.cluster_id` or
//! a peer address ends it. Refusals are counted, each refused peer is logged
//! once while it keeps being refused, and readiness lists the refused peers
//! that this node has configured ([`PeerCluster::mismatches`]). None of it
//! changes readiness: a misconfigured node must not take its healthy peers
//! out of service, nor hide why a node is unready.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tonic::metadata::AsciiMetadataValue;
use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use ursula_headers::ClusterId;

use crate::format_epoch::PROTOCOL_MISMATCH_TEXT;
use crate::peer_directory::PeerDirectory;
use crate::rt::time::Instant;

/// The format epoch that introduced cluster ids on Raft RPCs: a sender that
/// names none is a node of an earlier epoch.
const CLUSTER_ID_EPOCH: u32 = 4;

/// Request metadata naming the sender's cluster id; on a refusal, the
/// refusing node's.
pub const CLUSTER_ID_METADATA: &str = "ursula-cluster-id";

/// Request metadata naming the sender's Raft node id.
pub const NODE_ID_METADATA: &str = "ursula-node-id";

/// How long a refused peer stays in the report after its last refusal. A
/// misconfigured peer retries far more often (heartbeats every 250 ms,
/// elections within 3 s), so it stays listed while it keeps trying, and drops
/// out once an operator fixed it.
pub const PEER_CLUSTER_MISMATCH_TTL: Duration = Duration::from_secs(60);

/// The most refused senders a node remembers. Inbound keys come from request
/// metadata anyone who reaches the Raft plane can send, so the record is
/// bounded: past the cap the oldest entry goes.
pub const PEER_CLUSTER_MISMATCH_CAP: usize = 32;

#[derive(Debug)]
struct IdentityValues {
    node_id: u64,
    cluster_id: ClusterId,
    cluster_value: AsciiMetadataValue,
    node_value: AsciiMetadataValue,
}

/// Who this node is to its peers: the cluster id its WAL root holds and its
/// Raft node id. As a client interceptor it stamps both on every request.
/// Cloning it is cheap: every Raft client holds one.
#[derive(Debug, Clone)]
pub struct PeerIdentity(Arc<IdentityValues>);

impl PeerIdentity {
    pub fn new(node_id: u64, cluster_id: ClusterId) -> Self {
        Self(Arc::new(IdentityValues {
            node_id,
            cluster_value: cluster_id_value(&cluster_id),
            node_value: AsciiMetadataValue::from(node_id),
            cluster_id,
        }))
    }

    pub fn node_id(&self) -> u64 {
        self.0.node_id
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.0.cluster_id
    }
}

impl PartialEq for PeerIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.0.node_id == other.0.node_id && self.0.cluster_id == other.0.cluster_id
    }
}

impl Eq for PeerIdentity {}

impl Interceptor for PeerIdentity {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let metadata = request.metadata_mut();
        metadata.insert(CLUSTER_ID_METADATA, self.0.cluster_value.clone());
        metadata.insert(NODE_ID_METADATA, self.0.node_value.clone());
        Ok(request)
    }
}

/// A cluster id as a metadata value. Its grammar (`[A-Za-z0-9._-]`) is
/// visible ASCII, which every metadata value accepts.
fn cluster_id_value(cluster_id: &ClusterId) -> AsciiMetadataValue {
    AsciiMetadataValue::try_from(cluster_id.as_str())
        .expect("a cluster id is visible ASCII, a valid metadata value")
}

/// Why a peer's Raft RPC was refused before it ran. Every refusal is final
/// for the sender: only a configuration change ends it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PeerClusterError {
    /// No WAL bound a cluster id to this node (it runs without Raft
    /// persistence), so it serves no peers. Answered `UNAVAILABLE`, so a
    /// sender backs off as from a peer that is down.
    #[error("this node has no cluster id and serves no Raft peers")]
    NoLocalCluster,
    /// A sender that names no cluster id is a node before format epoch 4.
    /// The message carries [`PROTOCOL_MISMATCH_TEXT`], which such a node
    /// reads as a format-epoch refusal (E8).
    #[error(
        "{PROTOCOL_MISMATCH_TEXT}: the sender named no cluster id (a node before format epoch \
         {CLUSTER_ID_EPOCH}; a node joins only a cluster of its own format epoch)"
    )]
    MissingClusterId,
    #[error("the sender's {header} request metadata is malformed")]
    MalformedMetadata { header: &'static str },
    /// The peer belongs to another cluster.
    #[error(
        "peer cluster mismatch: node {peer_node_id} is in cluster {peer_cluster_id}, this node is \
         in cluster {local_cluster_id}; check server.cluster_id and raft.peers on both nodes"
    )]
    Mismatch {
        peer_node_id: u64,
        peer_cluster_id: ClusterId,
        local_cluster_id: ClusterId,
    },
}

impl From<PeerClusterError> for tonic::Status {
    fn from(error: PeerClusterError) -> Self {
        let message = error.to_string();
        match error {
            PeerClusterError::NoLocalCluster => Self::unavailable(message),
            PeerClusterError::MissingClusterId => Self::failed_precondition(message),
            PeerClusterError::MalformedMetadata { .. } => Self::invalid_argument(message),
            PeerClusterError::Mismatch {
                local_cluster_id, ..
            } => {
                let mut metadata = MetadataMap::new();
                metadata.insert(CLUSTER_ID_METADATA, cluster_id_value(&local_cluster_id));
                Self::with_metadata(tonic::Code::FailedPrecondition, message, metadata)
            }
        }
    }
}

/// The cluster id of the peer that refused an outbound RPC because it is in
/// another cluster, read from the refusal's metadata. `None` for every other
/// status, a protocol-version refusal
/// ([`crate::format_epoch::is_protocol_mismatch`]) among them even when it
/// carries that metadata.
pub(crate) fn refusing_cluster(status: &tonic::Status) -> Option<ClusterId> {
    if status.code() != tonic::Code::FailedPrecondition
        || crate::format_epoch::is_protocol_mismatch(status)
    {
        return None;
    }
    let value = status.metadata().get(CLUSTER_ID_METADATA)?.to_str().ok()?;
    ClusterId::new(value).ok()
}

pub use ursula_proto::admin::PeerClusterMismatch;

/// A sender this node refused, or a peer that refused it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RefusedPeer {
    /// A configured peer refused this node's request (`address` is the one
    /// this node dialed).
    Outbound {
        node_id: u64,
        cluster_id: ClusterId,
        address: String,
    },
    /// This node refused a request from `ip` whose sender claimed to be
    /// `node_id` of `cluster_id`.
    Inbound {
        node_id: u64,
        cluster_id: ClusterId,
        ip: Option<IpAddr>,
    },
    /// This node refused a request from `ip` that named no cluster id.
    Unidentified { ip: Option<IpAddr> },
}

impl RefusedPeer {
    /// Whether both name the same peer: the same node and cluster in either
    /// direction, or the same address for a sender without a cluster id.
    fn same_peer(&self, other: &Self) -> bool {
        match (self.peer(), other.peer()) {
            (Ok(left), Ok(right)) => left == right,
            (Err(left), Err(right)) => left == right,
            _ => false,
        }
    }

    fn peer(&self) -> Result<(u64, &ClusterId), Option<IpAddr>> {
        match self {
            Self::Outbound {
                node_id,
                cluster_id,
                ..
            }
            | Self::Inbound {
                node_id,
                cluster_id,
                ..
            } => Ok((*node_id, cluster_id)),
            Self::Unidentified { ip } => Err(*ip),
        }
    }
}

#[derive(Debug, Default)]
struct PeerClusterState {
    /// This node's identity and `raft.peers`, bound together once a WAL
    /// opens, so a reader sees both or neither.
    bound: arc_swap::ArcSwapOption<Bound>,
    /// Last refusal of each refused peer, at most
    /// [`PEER_CLUSTER_MISMATCH_CAP`].
    refused: Mutex<HashMap<RefusedPeer, Instant>>,
    refusals: AtomicU64,
    unidentified_refusals: AtomicU64,
    /// Member node ids `raft.peers` does not list, each logged once.
    unlisted: Mutex<BTreeSet<u64>>,
}

/// What [`PeerCluster::bind`] stores: this node's identity and its
/// configured peers, this node among them.
#[derive(Debug)]
struct Bound {
    identity: PeerIdentity,
    peers: PeerDirectory,
}

/// This node's side of the cluster check, shared by its registry, its
/// outbound transports and its Raft gRPC server: its identity and configured
/// peers once a WAL binds them, and the peers refused for another cluster id.
#[derive(Debug, Clone, Default)]
pub struct PeerCluster(Arc<PeerClusterState>);

impl PeerCluster {
    /// Binds this node's identity and its configured peers (`raft.peers`).
    /// The engine factory binds the identity of the WAL it opens groups
    /// over, before any group starts.
    pub fn bind(&self, identity: PeerIdentity, peers: PeerDirectory) {
        self.0
            .bound
            .store(Some(Arc::new(Bound { identity, peers })));
    }

    /// The configured peers last bound, this node among them.
    pub fn directory(&self) -> PeerDirectory {
        self.0
            .bound
            .load()
            .as_deref()
            .map(|bound| bound.peers.clone())
            .unwrap_or_default()
    }

    /// The cluster URL `raft.peers` lists for `node_id`, the only address
    /// this node dials it at. A Raft member that `raft.peers` does not list
    /// has none: nothing dials it, and the first miss for each node id is
    /// logged.
    pub fn cluster_url(&self, node_id: u64) -> Option<String> {
        let bound = self.0.bound.load();
        let url = bound
            .as_deref()
            .and_then(|bound| bound.peers.cluster_url(node_id))
            .map(str::to_owned);
        if url.is_none()
            && self
                .0
                .unlisted
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(node_id)
        {
            tracing::warn!(
                peer_node_id = node_id,
                "raft.peers lists no node {node_id}, a member of a Raft group: not dialing it; \
                 list the node in raft.peers or remove it from the group's membership"
            );
        }
        url
    }

    pub fn identity(&self) -> Option<PeerIdentity> {
        self.0
            .bound
            .load()
            .as_deref()
            .map(|bound| bound.identity.clone())
    }

    /// The interceptor a Raft gRPC server checks every request with.
    pub fn server_check(&self) -> ClusterCheck {
        ClusterCheck(self.clone())
    }

    /// Admits an inbound request from `remote`, at `now`, whose metadata
    /// names this node's cluster.
    pub(crate) fn admit(
        &self,
        metadata: &MetadataMap,
        remote: Option<SocketAddr>,
        now: Instant,
    ) -> Result<(), PeerClusterError> {
        let bound = self.0.bound.load();
        let Some(local) = bound.as_deref().map(|bound| &bound.identity) else {
            return Err(PeerClusterError::NoLocalCluster);
        };
        let ip = remote.map(|remote| remote.ip());
        let Some(cluster_value) = metadata.get(CLUSTER_ID_METADATA) else {
            self.0.unidentified_refusals.fetch_add(1, Ordering::Relaxed);
            if self.record(RefusedPeer::Unidentified { ip }, now) {
                tracing::error!(
                    peer_address = ?remote,
                    "refusing Raft traffic from a sender that names no cluster id (a node \
                     before format epoch {CLUSTER_ID_EPOCH}, or a stray client); check the peer \
                     lists of the nodes at that address"
                );
            }
            return Err(PeerClusterError::MissingClusterId);
        };
        let malformed = |header| PeerClusterError::MalformedMetadata { header };
        let peer_node_id = metadata
            .get(NODE_ID_METADATA)
            .and_then(|value| value.to_str().ok())
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or_else(|| malformed(NODE_ID_METADATA))?;
        // The common case compares bytes; only a mismatch is parsed.
        if cluster_value.as_bytes() == local.0.cluster_value.as_bytes() {
            return Ok(());
        }
        let cluster_id = cluster_value
            .to_str()
            .ok()
            .and_then(|text| ClusterId::new(text).ok())
            .ok_or_else(|| malformed(CLUSTER_ID_METADATA))?;
        if cluster_id == local.0.cluster_id {
            return Ok(());
        }
        self.0.refusals.fetch_add(1, Ordering::Relaxed);
        if self.record(
            RefusedPeer::Inbound {
                node_id: peer_node_id,
                cluster_id: cluster_id.clone(),
                ip,
            },
            now,
        ) {
            tracing::error!(
                direction = "inbound",
                peer_node_id,
                peer_cluster_id = %cluster_id,
                peer_address = ?remote,
                local_cluster_id = %local.0.cluster_id,
                "refusing Raft traffic with a peer of another cluster: check server.cluster_id \
                 and raft.peers on both nodes"
            );
        }
        Err(PeerClusterError::Mismatch {
            peer_node_id,
            peer_cluster_id: cluster_id,
            local_cluster_id: local.0.cluster_id.clone(),
        })
    }

    /// Records `status` when peer `node_id` at `address` refused an outbound
    /// RPC because it is in another cluster. Returns whether it did.
    pub(crate) fn observe_refusal(
        &self,
        node_id: u64,
        address: &str,
        status: &tonic::Status,
        now: Instant,
    ) -> bool {
        let Some(cluster_id) = refusing_cluster(status) else {
            return false;
        };
        self.0.refusals.fetch_add(1, Ordering::Relaxed);
        if self.record(
            RefusedPeer::Outbound {
                node_id,
                cluster_id: cluster_id.clone(),
                address: address.to_owned(),
            },
            now,
        ) {
            tracing::error!(
                direction = "outbound",
                peer_node_id = node_id,
                peer_cluster_id = %cluster_id,
                peer_address = address,
                local_cluster_id = self.identity().as_ref().map(|identity| identity.cluster_id().as_str()),
                "refusing Raft traffic with a peer of another cluster: check server.cluster_id \
                 and raft.peers on both nodes"
            );
        }
        true
    }

    /// Whether configured peer `node_id` refused this node's traffic for
    /// another cluster id within [`PEER_CLUSTER_MISMATCH_TTL`].
    pub(crate) fn refuses(&self, node_id: u64, now: Instant) -> bool {
        self.lock().iter().any(|(peer, seen)| {
            matches!(peer, RefusedPeer::Outbound { node_id: id, .. } if *id == node_id)
                && now.saturating_duration_since(*seen) <= PEER_CLUSTER_MISMATCH_TTL
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RefusedPeer, Instant>> {
        self.0
            .refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a refusal of `peer`. Returns whether it is fresh (the same
    /// peer, either way, not refused within [`PEER_CLUSTER_MISMATCH_TTL`]),
    /// which the caller logs, so each refused peer is logged once while it
    /// keeps being refused.
    fn record(&self, peer: RefusedPeer, now: Instant) -> bool {
        let mut refused = self.lock();
        refused.retain(|_, seen| now.saturating_duration_since(*seen) <= PEER_CLUSTER_MISMATCH_TTL);
        let fresh = !refused.keys().any(|known| known.same_peer(&peer));
        if !refused.contains_key(&peer) && refused.len() >= PEER_CLUSTER_MISMATCH_CAP {
            let oldest = refused
                .iter()
                .min_by_key(|(_, seen)| **seen)
                .map(|(peer, _)| peer.clone());
            if let Some(oldest) = oldest {
                refused.remove(&oldest);
            }
        }
        refused.insert(peer, now);
        fresh
    }

    /// The configured peers refused for another cluster id within
    /// [`PEER_CLUSTER_MISMATCH_TTL`], either way, by node id. An inbound
    /// refusal counts when its sender claims a configured node id and, where
    /// that peer's URL names an IP address, sent from that address.
    pub fn mismatches(&self) -> Vec<PeerClusterMismatch> {
        self.mismatches_at(Instant::now())
    }

    /// [`Self::mismatches`] as of `now`.
    pub(crate) fn mismatches_at(&self, now: Instant) -> Vec<PeerClusterMismatch> {
        let bound = self.0.bound.load();
        let peers = bound.as_deref().map(|bound| &bound.peers);
        let local = bound.as_deref().map(|bound| bound.identity.node_id());
        let mut mismatches = self
            .lock()
            .iter()
            .filter(|(_, seen)| now.saturating_duration_since(**seen) <= PEER_CLUSTER_MISMATCH_TTL)
            .filter_map(|(peer, _)| match peer {
                RefusedPeer::Outbound {
                    node_id,
                    cluster_id,
                    ..
                } => Some((*node_id, cluster_id.clone())),
                RefusedPeer::Inbound {
                    node_id,
                    cluster_id,
                    ip,
                } => {
                    if Some(*node_id) == local {
                        return None;
                    }
                    let url = peers?.cluster_url(*node_id)?;
                    let matches_address = match (url_ip(url), ip) {
                        (Some(configured), Some(ip)) => configured == *ip,
                        _ => true,
                    };
                    matches_address.then(|| (*node_id, cluster_id.clone()))
                }
                RefusedPeer::Unidentified { .. } => None,
            })
            .collect::<Vec<_>>();
        mismatches
            .sort_by(|left, right| (left.0, left.1.as_str()).cmp(&(right.0, right.1.as_str())));
        mismatches.dedup();
        mismatches
            .into_iter()
            .map(|(node_id, cluster_id)| PeerClusterMismatch {
                node_id,
                cluster_id,
            })
            .collect()
    }

    /// Raft RPCs refused since start for a cluster-id mismatch, both ways.
    pub fn refusals(&self) -> u64 {
        self.0.refusals.load(Ordering::Relaxed)
    }

    /// Inbound Raft RPCs refused since start because they named no cluster
    /// id.
    pub fn unidentified_refusals(&self) -> u64 {
        self.0.unidentified_refusals.load(Ordering::Relaxed)
    }
}

/// The IP address a peer URL names, when it names one rather than a host
/// name (`http://10.0.0.3:4437`).
fn url_ip(url: &str) -> Option<IpAddr> {
    let authority = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = authority.split('/').next()?;
    if let Ok(address) = authority.parse::<SocketAddr>() {
        return Some(address.ip());
    }
    authority.parse::<IpAddr>().ok()
}

/// The cluster every in-crate test and simulation node is in.
#[cfg(any(test, madsim))]
pub(crate) const TEST_CLUSTER_ID: &str = "ursula-test";

/// Node `node_id` of the test cluster.
#[cfg(any(test, madsim))]
pub(crate) fn test_identity(node_id: u64) -> PeerIdentity {
    PeerIdentity::new(
        node_id,
        ClusterId::new(TEST_CLUSTER_ID).expect("a valid cluster id"),
    )
}

/// A node of the test cluster with no configured peers.
#[cfg(any(test, madsim))]
pub(crate) fn test_peer_cluster(node_id: u64) -> PeerCluster {
    test_peer_cluster_with(node_id, PeerDirectory::default())
}

/// A node of the test cluster whose `raft.peers` is `peers`.
#[cfg(any(test, madsim))]
pub(crate) fn test_peer_cluster_with(node_id: u64, peers: PeerDirectory) -> PeerCluster {
    let peer_cluster = PeerCluster::default();
    peer_cluster.bind(test_identity(node_id), peers);
    peer_cluster
}

/// The server interceptor: admits a request only from a peer of this node's
/// cluster ([`PeerCluster::admit`]).
#[derive(Debug, Clone)]
pub struct ClusterCheck(PeerCluster);

impl Interceptor for ClusterCheck {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        self.0
            .admit(request.metadata(), request.remote_addr(), Instant::now())?;
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use super::ClusterCheck;
    use super::ClusterId;
    use super::Interceptor;
    use super::PEER_CLUSTER_MISMATCH_CAP;
    use super::PEER_CLUSTER_MISMATCH_TTL;
    use super::PeerCluster;
    use super::PeerClusterError;
    use super::PeerClusterMismatch;
    use super::PeerIdentity;
    use super::RefusedPeer;
    use super::refusing_cluster;
    use super::url_ip;
    use crate::format_epoch::is_protocol_mismatch;
    use crate::peer_directory::PeerDirectory;
    use crate::peer_directory::PeerEndpoints;
    use crate::rt::time::Instant;

    fn cluster(id: &str) -> ClusterId {
        ClusterId::new(id).unwrap()
    }

    /// `t0` plus `offset`: the record only compares the instants it is given.
    fn at(t0: Instant, offset: Duration) -> Instant {
        t0.checked_add(offset).unwrap()
    }

    /// Node `node_id` of cluster `id`, whose peers 1 to 3 listen on
    /// 10.0.0.1 to 10.0.0.3.
    fn bound(node_id: u64, id: &str) -> PeerCluster {
        let peer_cluster = PeerCluster::default();
        let peers = PeerDirectory::new((1..=3).map(|peer| {
            (
                peer,
                PeerEndpoints::shared(format!("http://10.0.0.{peer}:4437")),
            )
        }));
        peer_cluster.bind(PeerIdentity::new(node_id, cluster(id)), peers);
        peer_cluster
    }

    fn from_node(node_id: u64) -> Option<SocketAddr> {
        Some(format!("10.0.0.{node_id}:50000").parse().unwrap())
    }

    /// A request stamped by `sender`, as its client sends it.
    fn stamped(sender: &PeerIdentity) -> tonic::Request<()> {
        sender.clone().call(tonic::Request::new(())).unwrap()
    }

    /// The server's refusal of `request`, `None` when it admits it.
    fn refusal(server: &PeerCluster, request: tonic::Request<()>) -> Option<tonic::Status> {
        ClusterCheck(server.clone()).call(request).err()
    }

    fn stray(node_id: u64) -> RefusedPeer {
        RefusedPeer::Inbound {
            node_id,
            cluster_id: cluster("b"),
            ip: None,
        }
    }

    #[test]
    fn the_client_stamps_its_cluster_and_node_id() {
        let request = stamped(&PeerIdentity::new(7, cluster("prod-eu-1")));
        let metadata = request.metadata();
        assert_eq!(
            metadata.get(super::CLUSTER_ID_METADATA).unwrap(),
            "prod-eu-1"
        );
        assert_eq!(metadata.get(super::NODE_ID_METADATA).unwrap(), "7");
    }

    #[test]
    fn a_peer_of_the_same_cluster_is_admitted() {
        let server = bound(1, "prod-eu-1");
        let request = stamped(&PeerIdentity::new(2, cluster("prod-eu-1")));
        assert!(refusal(&server, request).is_none());
        assert!(server.mismatches_at(Instant::now()).is_empty());
        assert_eq!(server.refusals(), 0);
    }

    #[test]
    fn a_peer_of_another_cluster_is_refused_counted_and_reported() {
        let t0 = Instant::now();
        let server = bound(1, "prod-eu-1");
        let sender = PeerIdentity::new(3, cluster("prod-us-1"));
        let error = server
            .admit(stamped(&sender).metadata(), from_node(3), t0)
            .unwrap_err();
        assert_eq!(error, PeerClusterError::Mismatch {
            peer_node_id: 3,
            peer_cluster_id: cluster("prod-us-1"),
            local_cluster_id: cluster("prod-eu-1"),
        });
        // Every refusal counts; the peer is reported once.
        let status = tonic::Status::from(
            server
                .admit(stamped(&sender).metadata(), from_node(3), t0)
                .unwrap_err(),
        );
        assert_eq!(server.refusals(), 2);
        assert_eq!(server.mismatches_at(t0), vec![PeerClusterMismatch {
            node_id: 3,
            cluster_id: cluster("prod-us-1"),
        }]);
        // The refusal names this node's cluster, so the sender records it
        // under the refusing node's id; it is not a format-epoch refusal.
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(!is_protocol_mismatch(&status));
        assert_eq!(refusing_cluster(&status), Some(cluster("prod-eu-1")));
        let client = bound(3, "prod-us-1");
        assert!(client.observe_refusal(1, "http://10.0.0.1:4437", &status, t0));
        assert!(client.refuses(1, t0));
        assert!(!client.refuses(2, t0));
        assert!(!client.refuses(1, at(t0, PEER_CLUSTER_MISMATCH_TTL * 2)));
        assert_eq!(client.mismatches_at(t0), vec![PeerClusterMismatch {
            node_id: 1,
            cluster_id: cluster("prod-eu-1"),
        }]);
        assert_eq!(client.refusals(), 1);
    }

    #[test]
    fn only_configured_peers_are_reported() {
        let t0 = Instant::now();
        let server = bound(1, "a");
        // Claims node 2 but sends from another address than node 2's.
        let impostor = PeerIdentity::new(2, cluster("b"));
        server
            .admit(stamped(&impostor).metadata(), from_node(9), t0)
            .unwrap_err();
        // A node id this node does not configure.
        let stranger = PeerIdentity::new(7, cluster("b"));
        server
            .admit(stamped(&stranger).metadata(), from_node(7), t0)
            .unwrap_err();
        assert!(server.mismatches_at(t0).is_empty());
        assert_eq!(server.refusals(), 2);
        // The same claim from node 2's address is reported.
        server
            .admit(stamped(&impostor).metadata(), from_node(2), t0)
            .unwrap_err();
        assert_eq!(server.mismatches_at(t0), vec![PeerClusterMismatch {
            node_id: 2,
            cluster_id: cluster("b"),
        }]);
    }

    #[test]
    fn each_refused_peer_is_fresh_once_until_it_stops_being_refused() {
        let t0 = Instant::now();
        let server = bound(1, "a");
        assert!(server.record(stray(2), t0), "the first refusal is logged");
        assert!(!server.record(stray(2), t0), "a repeat is not");
        assert!(
            !server.record(
                RefusedPeer::Outbound {
                    node_id: 2,
                    cluster_id: cluster("b"),
                    address: "http://10.0.0.2:4437".to_owned(),
                },
                t0,
            ),
            "nor the same peer refusing this node"
        );
        assert!(server.record(
            RefusedPeer::Inbound {
                node_id: 2,
                cluster_id: cluster("c"),
                ip: None,
            },
            t0,
        ));
    }

    #[test]
    fn a_peer_drops_out_of_the_report_once_it_stops_being_refused() {
        let t0 = Instant::now();
        let half = PEER_CLUSTER_MISMATCH_TTL / 2;
        let server = bound(1, "a");
        let sender = PeerIdentity::new(2, cluster("b"));
        let request = |now| {
            server
                .admit(stamped(&sender).metadata(), from_node(2), now)
                .unwrap_err()
        };
        request(t0);
        request(at(t0, half));
        assert_eq!(
            server
                .mismatches_at(at(t0, PEER_CLUSTER_MISMATCH_TTL))
                .len(),
            1,
            "refused within the window"
        );
        let later = at(t0, half.saturating_add(PEER_CLUSTER_MISMATCH_TTL * 2));
        assert!(server.mismatches_at(later).is_empty());
        assert_eq!(server.refusals(), 2);
        // Refused again after the window, the peer is fresh again.
        assert!(server.record(stray(2), later));
    }

    #[test]
    fn the_record_is_capped_and_evicts_the_oldest() {
        let t0 = Instant::now();
        let server = bound(1, "a");
        for node_id in 0..100 {
            server.record(stray(node_id), at(t0, Duration::from_millis(node_id)));
        }
        let refused = server.lock();
        assert_eq!(refused.len(), PEER_CLUSTER_MISMATCH_CAP);
        assert!(!refused.contains_key(&stray(0)), "the oldest went first");
        assert!(!refused.contains_key(&stray(67)));
        assert!(refused.contains_key(&stray(68)));
        assert!(refused.contains_key(&stray(99)));
    }

    #[test]
    fn other_refusals_are_not_cluster_refusals() {
        let client = bound(1, "a");
        for status in [
            tonic::Status::failed_precondition("raft grpc protocol mismatch"),
            tonic::Status::not_found("raft group 7 is not registered on this node"),
            tonic::Status::unavailable("connect"),
        ] {
            assert!(
                !client.observe_refusal(2, "http://10.0.0.2:4437", &status, Instant::now()),
                "{status:?}"
            );
        }
        assert_eq!(client.refusals(), 0);
    }

    /// A sender without a cluster id gets a format-epoch refusal, which an
    /// older node reads as E8. It is counted on its own, not as a cluster
    /// mismatch, and `admit` does not latch this node's format-epoch
    /// readiness (the latch is process-wide, so tests running in parallel
    /// cannot observe its absence here).
    #[test]
    fn a_sender_without_a_cluster_id_is_refused_without_a_latch() {
        let server = bound(1, "a");
        let status = refusal(&server, tonic::Request::new(())).expect("refused");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(is_protocol_mismatch(&status), "{status:?}");
        assert!(
            status.message().contains("before format epoch 4"),
            "{status:?}"
        );
        assert_eq!(server.refusals(), 0);
        assert_eq!(server.unidentified_refusals(), 1);
        assert!(server.mismatches_at(Instant::now()).is_empty());
    }

    #[test]
    fn malformed_metadata_is_refused_as_invalid() {
        let server = bound(1, "a");
        let now = Instant::now();
        let mut no_node = tonic::Request::new(());
        no_node
            .metadata_mut()
            .insert(super::CLUSTER_ID_METADATA, "a".parse().unwrap());
        assert_eq!(
            server.admit(no_node.metadata(), None, now),
            Err(PeerClusterError::MalformedMetadata {
                header: super::NODE_ID_METADATA
            })
        );
        // A node id that does not parse is refused even from this cluster.
        let mut bad_node = stamped(&PeerIdentity::new(2, cluster("a")));
        bad_node
            .metadata_mut()
            .insert(super::NODE_ID_METADATA, "two".parse().unwrap());
        assert_eq!(
            server.admit(bad_node.metadata(), None, now),
            Err(PeerClusterError::MalformedMetadata {
                header: super::NODE_ID_METADATA
            })
        );
        let mut bad_cluster = stamped(&PeerIdentity::new(2, cluster("a")));
        bad_cluster
            .metadata_mut()
            .insert(super::CLUSTER_ID_METADATA, "a b".parse().unwrap());
        let status = refusal(&server, bad_cluster).expect("refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn a_node_without_a_cluster_id_serves_no_peers() {
        let server = PeerCluster::default();
        let error = server
            .admit(
                stamped(&PeerIdentity::new(2, cluster("a"))).metadata(),
                None,
                Instant::now(),
            )
            .unwrap_err();
        assert_eq!(error, PeerClusterError::NoLocalCluster);
        let status = tonic::Status::from(error);
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn peer_urls_name_an_address_only_when_they_name_an_ip() {
        assert_eq!(
            url_ip("http://10.0.0.3:4437"),
            Some("10.0.0.3".parse().unwrap())
        );
        assert_eq!(url_ip("http://[::1]:4437/"), Some("::1".parse().unwrap()));
        assert_eq!(url_ip("http://ursula-2.ursula:4437"), None);
    }
}
