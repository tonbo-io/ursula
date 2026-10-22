use std::path::Path;
use std::path::PathBuf;

use ursula_raft::PeerDirectory;

/// What the runtime runs: the in-memory development engine, or one Raft
/// node, which every node with a WAL is, a single node included. Every Raft
/// node runs one spawn path and one set of gates.
#[derive(Debug)]
pub enum Topology {
    /// No Raft: the in-memory engine of the zero-config development mode.
    /// Nothing survives the process.
    Development { group_count: usize },
    /// A Raft node: `node_id` among `peers` (`raft.peers`, which lists the
    /// node itself), running `group_count` groups over the journals of
    /// `wal`.
    Cluster {
        node_id: u64,
        peers: PeerDirectory,
        group_count: usize,
        wal: ClusterWal,
    },
}

impl Topology {
    pub fn group_count(&self) -> usize {
        match self {
            Self::Development { group_count } | Self::Cluster { group_count, .. } => *group_count,
        }
    }
}

/// Where a Raft node keeps its WAL: the directory whose `raft-log`
/// subdirectory holds the journals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterWal {
    /// `raft.wal.path`, kept across runs.
    Durable(PathBuf),
    /// No path is configured, which only a single node allows: a fresh
    /// temporary directory for this run. Only [`ClusterWal::close`] after
    /// a clean WAL shutdown removes it. An error return leaves it, because
    /// a core writer may still write to it, and a journal write that fails
    /// stops the process.
    Temporary(PathBuf),
}

impl ClusterWal {
    /// `raft.wal.path`, or, unset, a new temporary directory.
    pub fn resolve(wal: &ursula_config::WalConfig) -> std::io::Result<Self> {
        match &wal.path {
            Some(path) => Ok(Self::Durable(path.clone())),
            None => {
                let dir = tempfile::Builder::new()
                    .prefix("ursula-wal-")
                    .tempdir()?
                    .keep();
                tracing::info!(
                    path = %dir.display(),
                    "raft.wal.path is not set: the Raft WAL runs in a temporary directory that \
                     is removed after a clean shutdown"
                );
                Ok(Self::Temporary(dir))
            }
        }
    }

    /// The WAL directory.
    pub fn root(&self) -> &Path {
        let (Self::Durable(root) | Self::Temporary(root)) = self;
        root
    }

    /// The journal directory: the WAL directory's `raft-log` subdirectory.
    pub fn log_dir(&self) -> PathBuf {
        self.root().join(ursula_config::WalConfig::LOG_SUBDIR)
    }

    /// After the server stopped: removes a temporary WAL directory once its
    /// WAL shut down cleanly (`clean`). Only then has every core writer
    /// closed, so nothing writes to the directory any more.
    pub(crate) fn close(self, clean: bool) {
        let Self::Temporary(path) = self else {
            return;
        };
        if !clean {
            tracing::warn!(
                path = %path.display(),
                "kept the temporary Raft WAL directory: the WAL did not shut down cleanly"
            );
            return;
        }
        match std::fs::remove_dir_all(&path) {
            Ok(()) => tracing::info!(
                path = %path.display(),
                "removed the temporary Raft WAL directory"
            ),
            Err(err) => tracing::warn!(
                path = %path.display(),
                %err,
                "could not remove the temporary Raft WAL directory"
            ),
        }
    }
}
