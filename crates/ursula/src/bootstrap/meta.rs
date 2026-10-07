//! Start the independent durable meta authority before any data-plane actors.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_control::OperationCommand;
use ursula_control::OperationOutcome;
use ursula_control::ProcessIdentity;
use ursula_control::ProcessState;
use ursula_proto::admin::ProcessIncarnation;
use ursula_raft::MetaRaftError;
use ursula_raft::MetaRaftHandle;
use ursula_shard::RaftGroupId;

pub(crate) struct MetaAuthority {
    pub handle: MetaRaftHandle,
    pub process: ProcessIdentity,
    shutdown: oneshot::Sender<()>,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl MetaAuthority {
    pub async fn shutdown(self) -> Result<(), MetaRaftError> {
        let _closed = self.shutdown.send(());
        let raft_result = self.handle.shutdown().await;
        let server_result = self
            .server
            .await
            .map_err(|error| MetaRaftError::with_source("join meta server", error))
            .and_then(|result| {
                result.map_err(|error| MetaRaftError::with_source("stop meta server", error))
            });
        server_result.and(raft_result)
    }
}

pub(crate) async fn start_meta_authority(
    config: &ursula_config::UrsulaConfig,
    root: PathBuf,
    incarnation: ProcessIncarnation,
) -> Result<MetaAuthority, MetaRaftError> {
    let raft_config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
            max_in_snapshot_log_to_keep: 16,
            ..Default::default()
        }
        .validate()
        .map_err(|error| MetaRaftError::with_source("meta raft configuration", error))?,
    );
    let listener = tokio::net::TcpListener::bind(&config.raft.meta.listen)
        .await
        .map_err(|error| MetaRaftError::with_source("bind meta listener", error))?;
    let handle = MetaRaftHandle::new_durable_recovering(
        config.raft.node_id,
        root,
        raft_config,
        incarnation.clone(),
    )
    .await?;
    let (shutdown, stopped) = oneshot::channel();
    let service = ursula_raft::MetaGrpcService::new(handle.clone());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _closed = stopped.await;
            })
            .await
    });
    let start = async {
        let peers: BTreeMap<_, _> = config
            .raft
            .meta
            .peers
            .iter()
            .map(|peer| (peer.node_id, BasicNode::new(&peer.url)))
            .collect();
        let data_peers = if config.raft.peers.is_empty() {
            vec![ursula_config::RaftPeerConfig {
                node_id: config.raft.node_id,
                url: format!("http://{}", config.server.listen),
            }]
        } else {
            config.raft.peers.clone()
        };
        let initializer = peers.keys().next().copied() == Some(config.raft.node_id);
        while !handle.replication_enabled() {
            let mut statuses = BTreeMap::new();
            for peer in &config.raft.meta.peers {
                if let Ok(status) = ursula_raft::meta_peer_recovery_status(&peer.url).await {
                    statuses.insert(peer.node_id, status);
                }
            }
            let all_empty = statuses.len() == peers.len()
                && statuses
                    .values()
                    .all(|status| !status.initialized && status.vote.is_none());
            if initializer
                && (config.raft.init_membership || config.raft.init_membership_per_group)
                && all_empty
            {
                // Permits bind to each receiver's fresh boot. A delayed genesis
                // RPC cannot open a replacement process after a disk loss.
                for peer in &config.raft.meta.peers {
                    let nonce = statuses
                        .get(&peer.node_id)
                        .and_then(|status| status.nonce.clone())
                        .ok_or_else(|| {
                            MetaRaftError::new("meta genesis", "peer lacks recovery nonce")
                        })?;
                    ursula_raft::meta_authorize_genesis(&peer.url, nonce).await?;
                }
                handle.initialize_membership(peers.clone()).await?;
                break;
            }
            // A leader ReadIndex uses the current (possibly joint) membership.
            // Sampling a count from static bootstrap peers would be unsafe after
            // decommission, because a removed stale voter is no longer evidence.
            for peer in &config.raft.meta.peers {
                if peer.node_id == config.raft.node_id {
                    continue;
                }
                if let Ok(floor) =
                    ursula_raft::meta_peer_recovery_floor(&peer.url, config.raft.node_id).await
                {
                    handle.install_recovery_vote_floor(floor).await?;
                    break;
                }
            }
            if !handle.replication_enabled() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        // This listener remains available while other nodes start their meta
        // replicas. No data workers or operator mutation listeners exist yet.
        let initial = loop {
            match handle.read_linearizable_state().await {
                Ok(state) => break state,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        };
        if initializer && initial.operations.processes.is_empty() {
            for peer in &data_peers {
                if !initial.nodes.contains_key(&peer.node_id) {
                    require_ok(
                        handle
                            .register_node(
                                ursula_raft::MetaNodeRegistration::new(
                                    peer.node_id,
                                    &peer.url,
                                    &peer.url,
                                ),
                                now_ms(),
                            )
                            .await?,
                    )?;
                }
            }
            for group in 0..config.raft.group_count {
                let group = RaftGroupId(
                    u32::try_from(group)
                        .map_err(|error| MetaRaftError::with_source("meta group id", error))?,
                );
                if initial.placements.contains_key(&group) {
                    continue;
                }
                let voters = config
                    .raft
                    .groups
                    .iter()
                    .find(|entry| entry.raft_group_id == group.0)
                    .map(|entry| entry.voters.iter().copied().collect())
                    .unwrap_or_else(|| data_peers.iter().map(|peer| peer.node_id).collect());
                require_ok(
                    handle
                        .commit_placement(group, voters, BTreeSet::new(), BTreeSet::new(), now_ms())
                        .await?,
                )?;
            }
        }
        let mut claim_epoch = None;
        loop {
            // Startup spans elections and other nodes' first claims. Read-only
            // quorum failures are retried inside the enclosing startup deadline.
            let state = match handle.read_linearizable_state().await {
                Ok(state) => state,
                Err(error) => {
                    tracing::debug!(%error, "waiting for startup meta quorum");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if !state.nodes.contains_key(&config.raft.node_id)
                || state.placements.len() < config.raft.group_count
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            // An RPC may lose its response after committing our claim. Observe
            // that exact boot identity before retrying, never increment blindly.
            if let Some(ProcessState::Active(identity)) =
                state.operations.processes.get(&config.raft.node_id)
                && identity.incarnation == incarnation
            {
                handle.set_process_identity(identity.clone())?;
                handle.raft_handle().runtime_config().elect(true);
                return Ok(identity.clone());
            }
            let expected_epoch = state
                .operations
                .processes
                .get(&config.raft.node_id)
                .map_or(0, ProcessState::epoch);
            if claim_epoch.is_some_and(|previous| previous != expected_epoch) {
                return Err(MetaRaftError::new(
                    "claim process boot",
                    "another process changed the pinned startup epoch",
                ));
            }
            claim_epoch = Some(expected_epoch);
            match handle
                .write(ControlCommand::Operation {
                    command: OperationCommand::ClaimProcess {
                        node_id: config.raft.node_id,
                        expected_epoch,
                        incarnation: incarnation.clone(),
                    },
                    now_ms: now_ms(),
                })
                .await
            {
                Ok(ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(identity)))) => {
                    handle.set_process_identity(identity.clone())?;
                    handle.raft_handle().runtime_config().elect(true);
                    return Ok(identity);
                }
                Err(error) => {
                    tracing::debug!(%error, "checking whether this startup claim committed")
                }
                Ok(ControlResponse::Operation(Err(
                    ursula_control::OperationError::ProcessChanged { .. },
                ))) => (),
                Ok(response) => {
                    return Err(MetaRaftError::new(
                        "claim process boot",
                        format!("{response:?}"),
                    ));
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    match tokio::time::timeout(Duration::from_secs(120), start).await {
        Ok(Ok(process)) => Ok(MetaAuthority {
            handle,
            process,
            shutdown,
            server,
        }),
        outcome => {
            let _closed = shutdown.send(());
            let _stopped = handle.shutdown().await;
            let _stopped = server.await;
            match outcome {
                Ok(Err(error)) => Err(error),
                Err(error) => Err(MetaRaftError::with_source("meta bootstrap timeout", error)),
                Ok(Ok(_)) => unreachable!("successful startup returned above"),
            }
        }
    }
}

fn require_ok(response: ControlResponse) -> Result<(), MetaRaftError> {
    if response == ControlResponse::Ok {
        Ok(())
    } else {
        Err(MetaRaftError::new(
            "initialize meta topology",
            format!("{response:?}"),
        ))
    }
}
fn now_ms() -> u64 {
    crate::unix_time_ms()
}
