//! Startup format-epoch gate.
//!
//! Runs before the runtime is spawned, in an order that writes nothing until
//! every check has passed:
//!
//! 1. classify the Raft WAL directory and the object-storage namespace
//!    read-only (E1-E4), and refuse a configured `server.cluster_id` that
//!    the WAL directory does not hold;
//! 2. probe every configured peer's Raft protocol (E8), as this node of its
//!    configured cluster; a peer of another cluster is logged, not fatal;
//! 3. only then write the missing markers: object storage first, then the
//!    directory.
//!
//! So a refused node exits non-zero without stamping a directory or a
//! namespace that a live cluster of another epoch still uses.

use std::io;
use std::path::Path;
use std::time::Duration;

use ursula_raft::PeerDirectory;
use ursula_raft::PeerFormatEpoch;
use ursula_raft::PeerIdentity;
use ursula_runtime::format_marker::FormatEpochNamespace;
use ursula_runtime::format_marker::MarkerState;
use ursula_runtime::format_marker::classify_data_dir;
use ursula_runtime::format_marker::write_data_dir_marker;

/// Per-step timeout of each parallel startup peer probe: 2 s to connect and
/// 2 s for the request, so one probe takes at most about 4 s.
const PEER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Why a node refuses to start at the format-epoch gate.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StartupGateError {
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The WAL directory holds another cluster id than the configured one.
    #[error(transparent)]
    Wal(#[from] ursula_raft::RaftWalError),
    /// Configuration validation requires `server.cluster_id` with
    /// `raft.peers`; a configuration that skipped it cannot probe peers.
    #[error("server.cluster_id is required when raft.peers is set")]
    ClusterIdRequired,
    /// A configured peer refused this node's Raft protocol (E8).
    #[error(
        "peer {peer_id} ({peer_url}) refused Raft protocol {epoch}: {message}. Format epochs \
         differ; a node cannot join a cluster of another format epoch. Install this release as a \
         new cluster",
        epoch = ursula_runtime::FORMAT_EPOCH
    )]
    PeerFormatEpoch {
        peer_id: u64,
        peer_url: String,
        message: String,
    },
}

/// `peers` is the node's `raft.peers`. `data_dir` is the Raft WAL's journal
/// directory, if the node runs Raft.
pub(crate) async fn check_and_stamp_format_epoch(
    config: &ursula_config::UrsulaConfig,
    peers: &PeerDirectory,
    data_dir: Option<&Path>,
) -> Result<(), StartupGateError> {
    // 1. Read-only classification. A WAL that holds another cluster id
    // refuses the start here, before peers would see this node as one of
    // another cluster.
    let data_dir_state = data_dir.map(classify_data_dir).transpose()?;
    if let (Some(dir), Some(cluster_id)) = (data_dir, config.server.cluster_id.as_ref()) {
        ursula_raft::RaftWal::check_cluster_id(dir, cluster_id)?;
    }
    let namespace =
        FormatEpochNamespace::from_config(&config.storage.cold, &config.storage.snapshot)?;
    let namespace_state = match &namespace {
        Some(namespace) => Some(namespace.classify().await?),
        None => None,
    };

    // 2. Peer probe.
    probe_peers(config, peers).await?;

    // 3. Markers: object storage first, then the directory.
    if let (Some(namespace), Some(MarkerState::Fresh)) = (&namespace, &namespace_state) {
        namespace.write_marker().await?;
    }
    if let (Some(dir), Some(MarkerState::Fresh)) = (data_dir, &data_dir_state) {
        write_data_dir_marker(dir)?;
    }
    Ok(())
}

async fn probe_peers(
    config: &ursula_config::UrsulaConfig,
    peers: &PeerDirectory,
) -> Result<(), StartupGateError> {
    let peers = peers
        .iter()
        .filter(|(node_id, _)| *node_id != config.raft.node_id)
        .map(|(node_id, endpoints)| (node_id, endpoints.cluster_url()))
        .collect::<Vec<_>>();
    if peers.is_empty() {
        return Ok(());
    }
    // Peers check the cluster id before anything else, so the probe carries
    // the configured one (validation requires it once `raft.peers` is set).
    let Some(cluster_id) = config.server.cluster_id.clone() else {
        return Err(StartupGateError::ClusterIdRequired);
    };
    let identity = PeerIdentity::new(config.raft.node_id, cluster_id);
    let identity = &identity;
    let answers =
        futures_util::future::join_all(peers.iter().map(|&(peer_id, peer_url)| async move {
            (
                peer_id,
                peer_url,
                ursula_raft::probe_peer_format_epoch(peer_url, identity, PEER_PROBE_TIMEOUT).await,
            )
        }))
        .await;
    for (peer_id, peer_url, answer) in answers {
        match answer {
            PeerFormatEpoch::Compatible => {}
            PeerFormatEpoch::OtherCluster(peer_cluster_id) => {
                // A misconfigured peer must not keep the rest of the cluster
                // from starting. The Raft plane refuses its traffic and
                // readiness lists it under peer_cluster_mismatch.
                tracing::error!(
                    peer_id,
                    peer_url,
                    %peer_cluster_id,
                    local_cluster_id = %identity.cluster_id(),
                    "a configured peer is in another cluster; its Raft traffic is refused. \
                     Check server.cluster_id and raft.peers on both nodes"
                );
            }
            PeerFormatEpoch::Unreachable(reason) => {
                // A fresh cluster starts its pods in parallel; readiness
                // covers a peer that turns out to differ later.
                tracing::info!(
                    peer_id,
                    peer_url,
                    reason = %reason,
                    "format-epoch probe skipped an unreachable peer"
                );
            }
            PeerFormatEpoch::Refused(message) => {
                return Err(StartupGateError::PeerFormatEpoch {
                    peer_id,
                    peer_url: peer_url.to_owned(),
                    message,
                });
            }
        }
    }
    Ok(())
}
