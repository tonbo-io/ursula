//! Startup format gate (format epoch 2).
//!
//! Runs before the runtime is spawned, in an order that writes nothing until
//! every check has passed:
//!
//! 1. classify the Raft WAL directory and the object-storage namespace
//!    read-only (E1-E4);
//! 2. probe every configured peer's Raft protocol (E8);
//! 3. only then write the missing markers: object storage first, then the
//!    directory.
//!
//! So a refused node exits non-zero without stamping a directory or a
//! namespace that a live 0.5.x cluster still uses.

use std::io;
use std::time::Duration;

use ursula_raft::PeerFormatEpoch;
use ursula_runtime::format_marker::FormatEpochNamespace;
use ursula_runtime::format_marker::MarkerState;
use ursula_runtime::format_marker::classify_data_dir;
use ursula_runtime::format_marker::write_data_dir_marker;

/// Budget for the parallel startup peer probes.
const PEER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) async fn check_and_stamp_format_epoch(
    config: &ursula_config::UrsulaConfig,
) -> io::Result<()> {
    // 1. Read-only classification.
    let data_dir = config.raft.wal.resolved_path();
    let data_dir_state = data_dir.as_deref().map(classify_data_dir).transpose()?;
    let namespace =
        FormatEpochNamespace::from_config(&config.storage.cold, &config.storage.snapshot)?;
    let namespace_state = match &namespace {
        Some(namespace) => Some(namespace.classify().await?),
        None => None,
    };

    // 2. Peer probe.
    probe_peers(config).await?;

    // 3. Markers: object storage first, then the directory.
    if let (Some(namespace), Some(MarkerState::Fresh)) = (&namespace, &namespace_state) {
        namespace.write_marker().await?;
    }
    if let (Some(dir), Some(MarkerState::Fresh)) = (&data_dir, &data_dir_state) {
        write_data_dir_marker(dir)?;
    }
    Ok(())
}

async fn probe_peers(config: &ursula_config::UrsulaConfig) -> io::Result<()> {
    let peers = config
        .raft
        .peers
        .iter()
        .filter(|peer| peer.node_id != config.raft.node_id)
        .collect::<Vec<_>>();
    let answers = futures_util::future::join_all(peers.iter().map(|peer| async move {
        (
            peer,
            ursula_raft::probe_peer_format_epoch(&peer.url, PEER_PROBE_TIMEOUT).await,
        )
    }))
    .await;
    for (peer, answer) in answers {
        match answer {
            PeerFormatEpoch::Compatible => {}
            PeerFormatEpoch::Unreachable(reason) => {
                // A fresh cluster starts its pods in parallel; readiness
                // covers a peer that turns out to differ later.
                tracing::info!(
                    peer_id = peer.node_id,
                    peer_url = %peer.url,
                    reason = %reason,
                    "format-epoch probe skipped an unreachable peer"
                );
            }
            PeerFormatEpoch::Refused(message) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "peer {} ({}) refused Raft protocol {}: {message}. Format epochs \
                         differ; an Ursula 0.6 node cannot join a 0.5.x cluster. Install 0.6 as \
                         a new cluster",
                        peer.node_id,
                        peer.url,
                        ursula_runtime::FORMAT_EPOCH
                    ),
                ));
            }
        }
    }
    Ok(())
}
