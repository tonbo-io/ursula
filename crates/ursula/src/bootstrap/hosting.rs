//! What a server node hosts (design 4.1 to 4.3): placement decides it.
//!
//! The spawn path gives the engine factory the node's [`MetaPlacement`]
//! with an empty [`HostingSet`] ([`SpawnedHosting`]), so no data replica
//! opens before the hosting driver admits it. Once the meta runtime has
//! published its first view, [`SpawnedHosting::start`] opens the cores the
//! journals hold, binds the placement to the meta runtime and starts the
//! driver ([`HostingDriver`]) over the node's shard runtime ([`ServerHost`]).
//! No open failure stops the start: a group whose open fails stays closed
//! under its open back-off, and the driver opens it again.

use std::sync::Arc;

use futures_util::future::BoxFuture;
use ursula_raft::GroupHost;
use ursula_raft::HostingDriver;
use ursula_raft::HostingSet;
use ursula_raft::HostingTask;
use ursula_raft::MetaPlacement;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftWal;
use ursula_raft::meta::status::MetaStatusReader;
use ursula_runtime::RuntimeError;
use ursula_runtime::ShardRuntime;
use ursula_shard::RaftGroupId;

/// The node's placement inputs and the hosting set only its hosting driver
/// writes, from the spawn path until the driver starts.
#[derive(Debug)]
pub struct SpawnedHosting {
    pub(crate) placement: MetaPlacement,
    pub(crate) hosting: HostingSet,
}

impl SpawnedHosting {
    /// Opens the cores the journals of `wal` hold, off the async runtime,
    /// so the driver's first plan opens what they hold (4.3), then binds
    /// the placement to `meta` and starts the driver over `runtime`. The
    /// caller starts it once the meta runtime has published its first view.
    pub(crate) async fn start(
        self,
        runtime: ShardRuntime,
        registry: RaftGroupHandleRegistry,
        wal: RaftWal,
        meta: MetaStatusReader,
    ) -> HostingTask {
        let metrics = runtime.metrics().group_engine_metrics();
        let opening = wal.clone();
        match tokio::task::spawn_blocking(move || opening.open_held_cores(metrics)).await {
            Ok(Ok(_held)) => {}
            // Only a WAL that shut down or a poisoned lock fails the opens,
            // and then the driver plans nothing either.
            Ok(Err(error)) => {
                tracing::error!(%error, "hosting: the journals did not open their cores");
            }
            Err(error) => tracing::error!(%error, "hosting: the journals' open did not end"),
        }
        self.placement.bind_meta(meta.clone());
        HostingDriver::start(
            Arc::new(ServerHost { runtime, registry }),
            wal,
            meta,
            self.hosting,
        )
    }
}

/// The server's [`GroupHost`]: a replica opens as its first request opens
/// it, through the shard runtime's engine factory.
struct ServerHost {
    runtime: ShardRuntime,
    registry: RaftGroupHandleRegistry,
}

impl GroupHost for ServerHost {
    type Error = RuntimeError;

    fn open(&self, group: RaftGroupId) -> BoxFuture<'_, Result<(), RuntimeError>> {
        Box::pin(async move { self.runtime.warm_group(group).await.map(|_placement| ()) })
    }

    fn close(&self, group: RaftGroupId) -> BoxFuture<'_, Result<(), RuntimeError>> {
        Box::pin(async move {
            let placement = self.runtime.placement_for_group(group)?;
            self.registry.unregister_group(group);
            self.runtime.shutdown_group_engine(placement).await
        })
    }

    fn forget(&self, group: RaftGroupId) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Err(error) = self.runtime.forget_group(group).await {
                tracing::warn!(
                    group = group.0,
                    %error,
                    "hosting: the gauges of a closed replica were not forgotten"
                );
            }
        })
    }
}
