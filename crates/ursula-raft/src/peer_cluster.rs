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
//!   its [`CLUSTER_ID_METADATA`], so the sender learns it too. Both sides
//!   record the peer ([`PeerCluster::mismatches`]): `/__ursula/ready` lists it
//!   as `peer_cluster_mismatch`, and `/__ursula/metrics` counts the refusals;
//! - no id: a sender before format epoch 4, refused as a Raft protocol
//!   mismatch (E8, [`crate::format_epoch`]).
//!
//! A refusal is permanent: only an operator who fixes `server.cluster_id` or
//! a peer address ends it. Each peer is logged once while it keeps being
//! refused, not once per RPC.

use std::collections::HashMap;
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
use crate::format_epoch::record_format_epoch_mismatch;
use crate::rt::time::Instant;

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

/// Who this node is to its peers: the cluster id its WAL root holds and its
/// Raft node id. As a client interceptor it stamps both on every request.
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    node_id: u64,
    cluster_id: ClusterId,
    cluster_value: AsciiMetadataValue,
    node_value: AsciiMetadataValue,
}

impl PeerIdentity {
    pub fn new(node_id: u64, cluster_id: ClusterId) -> Self {
        Self {
            node_id,
            cluster_value: cluster_id_value(&cluster_id),
            node_value: AsciiMetadataValue::from(node_id),
            cluster_id,
        }
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
}

impl PartialEq for PeerIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.cluster_id == other.cluster_id
    }
}

impl Eq for PeerIdentity {}

impl Interceptor for PeerIdentity {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        let metadata = request.metadata_mut();
        metadata.insert(CLUSTER_ID_METADATA, self.cluster_value.clone());
        metadata.insert(NODE_ID_METADATA, self.node_value.clone());
        Ok(request)
    }
}

/// A cluster id as a metadata value. Its grammar (`[A-Za-z0-9._-]`) is
/// visible ASCII, which every metadata value accepts.
fn cluster_id_value(cluster_id: &ClusterId) -> AsciiMetadataValue {
    AsciiMetadataValue::try_from(cluster_id.as_str())
        .expect("a cluster id is visible ASCII, a valid metadata value")
}

/// Why a peer's Raft RPC was refused before it ran.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PeerClusterError {
    /// No WAL bound a cluster id to this node (it runs without Raft
    /// persistence), so it serves no peers.
    #[error("this node has no cluster id and serves no Raft peers")]
    NoLocalCluster,
    /// A sender that names no cluster id is a node before format epoch 4.
    /// The message carries [`PROTOCOL_MISMATCH_TEXT`], which such a node
    /// reads as a format-epoch refusal (E8).
    #[error(
        "{PROTOCOL_MISMATCH_TEXT}: the sender named no cluster id (a node before format epoch \
         {epoch}; a node joins only a cluster of its own format epoch)",
        epoch = ursula_stream::FORMAT_EPOCH
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

impl PeerClusterError {
    /// Whether a sender may succeed by retrying the same RPC. Only a node
    /// without Raft persistence answers retryably; every other refusal lasts
    /// until an operator changes a configuration.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::NoLocalCluster)
    }
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
/// status.
pub(crate) fn refusing_cluster(status: &tonic::Status) -> Option<ClusterId> {
    if status.code() != tonic::Code::FailedPrecondition {
        return None;
    }
    let value = status.metadata().get(CLUSTER_ID_METADATA)?.to_str().ok()?;
    ClusterId::new(value).ok()
}

/// A peer seen with another cluster id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerClusterMismatch {
    pub node_id: u64,
    pub cluster_id: ClusterId,
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    /// The peer's request was refused here.
    Inbound,
    /// The peer refused this node's request.
    Outbound,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

#[derive(Debug, Default)]
struct PeerClusterState {
    identity: arc_swap::ArcSwapOption<PeerIdentity>,
    /// Last refusal of each (peer node, peer cluster).
    mismatches: Mutex<HashMap<(u64, ClusterId), Instant>>,
    refusals: AtomicU64,
}

/// This node's side of the cluster check, shared by its registry, its
/// outbound transports and its Raft gRPC server: its identity once a WAL
/// binds one, and the peers refused for another cluster id.
#[derive(Debug, Clone, Default)]
pub struct PeerCluster(Arc<PeerClusterState>);

impl PeerCluster {
    /// Binds this node's identity. The engine factory binds the identity of
    /// the WAL it opens groups over, before any group starts.
    pub fn bind(&self, identity: PeerIdentity) {
        self.0.identity.store(Some(Arc::new(identity)));
    }

    pub fn identity(&self) -> Option<PeerIdentity> {
        self.0.identity.load().as_deref().cloned()
    }

    /// The interceptor a Raft gRPC server checks every request with.
    pub fn server_check(&self) -> ClusterCheck {
        ClusterCheck(self.clone())
    }

    /// Admits an inbound request whose metadata names this node's cluster.
    pub(crate) fn admit(&self, metadata: &MetadataMap) -> Result<(), PeerClusterError> {
        let Some(local) = self.0.identity.load_full() else {
            return Err(PeerClusterError::NoLocalCluster);
        };
        let Some(cluster_id) = metadata.get(CLUSTER_ID_METADATA) else {
            let error = PeerClusterError::MissingClusterId;
            record_format_epoch_mismatch("inbound", &error.to_string());
            return Err(error);
        };
        let malformed = |header| PeerClusterError::MalformedMetadata { header };
        let cluster_id = cluster_id
            .to_str()
            .ok()
            .and_then(|text| ClusterId::new(text).ok())
            .ok_or_else(|| malformed(CLUSTER_ID_METADATA))?;
        let peer_node_id = metadata
            .get(NODE_ID_METADATA)
            .and_then(|value| value.to_str().ok())
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or_else(|| malformed(NODE_ID_METADATA))?;
        if cluster_id == local.cluster_id {
            return Ok(());
        }
        self.record(Direction::Inbound, peer_node_id, cluster_id.clone());
        Err(PeerClusterError::Mismatch {
            peer_node_id,
            peer_cluster_id: cluster_id,
            local_cluster_id: local.cluster_id.clone(),
        })
    }

    /// Records `status` when peer `node_id` refused an outbound RPC because
    /// it is in another cluster. Returns whether it did.
    pub(crate) fn observe_refusal(&self, node_id: u64, status: &tonic::Status) -> bool {
        match refusing_cluster(status) {
            Some(cluster_id) => {
                self.record(Direction::Outbound, node_id, cluster_id);
                true
            }
            None => false,
        }
    }

    fn record(&self, direction: Direction, node_id: u64, cluster_id: ClusterId) {
        self.0.refusals.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let previous = self
            .0
            .mismatches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((node_id, cluster_id.clone()), now);
        let fresh = previous
            .is_none_or(|seen| now.saturating_duration_since(seen) > PEER_CLUSTER_MISMATCH_TTL);
        if fresh {
            let local = self.identity();
            tracing::error!(
                direction = direction.as_str(),
                peer_node_id = node_id,
                peer_cluster_id = %cluster_id,
                local_cluster_id = local.as_ref().map(|identity| identity.cluster_id.as_str()),
                "refusing Raft traffic with a peer of another cluster: check server.cluster_id \
                 and raft.peers on both nodes; /__ursula/ready lists the peer under \
                 peer_cluster_mismatch"
            );
        }
    }

    /// The peers refused within [`PEER_CLUSTER_MISMATCH_TTL`], by node id.
    pub fn mismatches(&self) -> Vec<PeerClusterMismatch> {
        let now = Instant::now();
        let mut mismatches = self
            .0
            .mismatches
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        mismatches
            .retain(|_, seen| now.saturating_duration_since(*seen) <= PEER_CLUSTER_MISMATCH_TTL);
        let mut recent = mismatches
            .keys()
            .map(|(node_id, cluster_id)| PeerClusterMismatch {
                node_id: *node_id,
                cluster_id: cluster_id.clone(),
            })
            .collect::<Vec<_>>();
        drop(mismatches);
        recent.sort_by(|left, right| {
            (left.node_id, left.cluster_id.as_str())
                .cmp(&(right.node_id, right.cluster_id.as_str()))
        });
        recent
    }

    /// Raft RPCs refused since start for a cluster-id mismatch, both ways.
    pub fn refusals(&self) -> u64 {
        self.0.refusals.load(Ordering::Relaxed)
    }
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

/// The server interceptor: admits a request only from a peer of this node's
/// cluster ([`PeerCluster::admit`]).
#[derive(Debug, Clone)]
pub struct ClusterCheck(PeerCluster);

impl Interceptor for ClusterCheck {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        self.0.admit(request.metadata())?;
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::ClusterCheck;
    use super::ClusterId;
    use super::Interceptor;
    use super::PeerCluster;
    use super::PeerClusterError;
    use super::PeerClusterMismatch;
    use super::PeerIdentity;
    use super::refusing_cluster;
    use crate::format_epoch::FormatEpochMismatch;
    use crate::format_epoch::is_protocol_mismatch;

    fn cluster(id: &str) -> ClusterId {
        ClusterId::new(id).unwrap()
    }

    fn bound(node_id: u64, id: &str) -> PeerCluster {
        let peer_cluster = PeerCluster::default();
        peer_cluster.bind(PeerIdentity::new(node_id, cluster(id)));
        peer_cluster
    }

    /// A request stamped by `sender`, as its client sends it.
    fn stamped(sender: &PeerIdentity) -> tonic::Request<()> {
        sender.clone().call(tonic::Request::new(())).unwrap()
    }

    /// The server's refusal of `request`, `None` when it admits it.
    fn refusal(server: &PeerCluster, request: tonic::Request<()>) -> Option<tonic::Status> {
        ClusterCheck(server.clone()).call(request).err()
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
        assert!(server.mismatches().is_empty());
        assert_eq!(server.refusals(), 0);
    }

    #[tokio::test]
    async fn a_peer_of_another_cluster_is_refused_counted_and_reported() {
        let server = bound(1, "prod-eu-1");
        let sender = PeerIdentity::new(3, cluster("prod-us-1"));
        let error = server.admit(stamped(&sender).metadata()).unwrap_err();
        assert_eq!(error, PeerClusterError::Mismatch {
            peer_node_id: 3,
            peer_cluster_id: cluster("prod-us-1"),
            local_cluster_id: cluster("prod-eu-1"),
        });
        assert!(!error.is_retryable());
        // Every refusal counts; the peer is reported once.
        let status = refusal(&server, stamped(&sender)).expect("refused");
        assert_eq!(server.refusals(), 2);
        assert_eq!(server.mismatches(), vec![PeerClusterMismatch {
            node_id: 3,
            cluster_id: cluster("prod-us-1"),
        }]);
        // The refusal names this node's cluster, so the sender records it
        // under the refusing node's id; it is not a format-epoch refusal.
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(!is_protocol_mismatch(&status));
        assert_eq!(refusing_cluster(&status), Some(cluster("prod-eu-1")));
        let client = bound(3, "prod-us-1");
        assert!(client.observe_refusal(1, &status));
        assert_eq!(client.mismatches(), vec![PeerClusterMismatch {
            node_id: 1,
            cluster_id: cluster("prod-eu-1"),
        }]);
        assert_eq!(client.refusals(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_drops_out_of_the_report_once_it_stops_being_refused() {
        let server = bound(1, "a");
        let sender = PeerIdentity::new(2, cluster("b"));
        refusal(&server, stamped(&sender)).expect("refused");
        tokio::time::advance(super::PEER_CLUSTER_MISMATCH_TTL / 2).await;
        refusal(&server, stamped(&sender)).expect("refused");
        tokio::time::advance(super::PEER_CLUSTER_MISMATCH_TTL / 2).await;
        assert_eq!(server.mismatches().len(), 1, "refused within the window");
        tokio::time::advance(super::PEER_CLUSTER_MISMATCH_TTL).await;
        assert!(server.mismatches().is_empty());
        assert_eq!(server.refusals(), 2);
    }

    #[test]
    fn other_refusals_are_not_cluster_refusals() {
        let client = bound(1, "a");
        for status in [
            tonic::Status::failed_precondition("raft grpc protocol mismatch"),
            tonic::Status::not_found("raft group 7 is not registered on this node"),
            tonic::Status::unavailable("connect"),
        ] {
            assert!(!client.observe_refusal(2, &status), "{status:?}");
        }
        assert_eq!(client.refusals(), 0);
    }

    #[test]
    fn a_sender_without_a_cluster_id_is_a_format_epoch_refusal() {
        let server = bound(1, "a");
        let status = refusal(&server, tonic::Request::new(())).expect("refused");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(is_protocol_mismatch(&status), "{status:?}");
        assert!(FormatEpochMismatch::global().recorded());
        assert_eq!(server.refusals(), 0);
    }

    #[test]
    fn malformed_metadata_is_refused_as_invalid() {
        let server = bound(1, "a");
        let mut no_node = tonic::Request::new(());
        no_node
            .metadata_mut()
            .insert(super::CLUSTER_ID_METADATA, "a".parse().unwrap());
        assert_eq!(
            server.admit(no_node.metadata()),
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
            .admit(stamped(&PeerIdentity::new(2, cluster("a"))).metadata())
            .unwrap_err();
        assert_eq!(error, PeerClusterError::NoLocalCluster);
        assert!(error.is_retryable());
        let status = tonic::Status::from(error);
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }
}
