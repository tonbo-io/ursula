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
use ursula_control::ReplicaState;
use ursula_proto::admin::ProcessIncarnation;
use ursula_proto::admin::ReplicaIdentity;
use ursula_raft::MetaRaftError;
use ursula_raft::MetaRaftHandle;
use ursula_raft::wal::ReplicaIdentityStore;
use ursula_shard::RaftGroupId;

pub(crate) struct MetaAuthority {
    pub handle: MetaRaftHandle,
    pub process: ProcessIdentity,
    pub replica: ReplicaIdentity,
    pub replica_genesis: BTreeMap<u64, ReplicaIdentity>,
    pub replica_genesis_prefixes: BTreeMap<RaftGroupId, u64>,
    _replica_store: ReplicaIdentityStore,
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
    let identity_root = config
        .raft
        .wal
        .path
        .as_ref()
        .ok_or_else(|| MetaRaftError::new("replica identity", "persistent WAL path required"))?
        .join(ursula_config::WalConfig::LOG_SUBDIR);
    let node_id = config.raft.node_id;
    let replica_store = tokio::task::spawn_blocking(move || {
        let fresh = ProcessIncarnation::from_bits(rand::random());
        ReplicaIdentityStore::open(&identity_root, node_id, fresh)
    })
    .await
    .map_err(|error| MetaRaftError::with_source("join replica identity load", error))?
    .map_err(|error| MetaRaftError::with_source("load replica identity", error))?;
    let raft_config = Arc::new(
        openraft::Config {
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
            max_in_snapshot_log_to_keep: 16,
            ..Default::default()
        }
        .validate()
        .map_err(|error| MetaRaftError::with_source("meta raft configuration", error))?,
    );
    let implicit_single = config.raft.uses_implicit_single_node_meta();
    let listen = if implicit_single {
        "127.0.0.1:0"
    } else {
        &config.raft.meta.listen
    };
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|error| MetaRaftError::with_source("bind meta listener", error))?;
    let meta_peers = if implicit_single {
        vec![ursula_config::RaftPeerConfig {
            node_id: config.raft.node_id,
            url: format!(
                "http://{}",
                listener
                    .local_addr()
                    .map_err(|error| MetaRaftError::with_source("meta listener address", error))?
            ),
        }]
    } else {
        config.raft.meta.peers.clone()
    };
    let rpc_auth = match &config.raft.meta.auth_token_file {
        Some(path) => ursula_raft::MetaRpcAuth::from_token_file(path)?,
        None if implicit_single => ursula_raft::MetaRpcAuth::default(),
        None => {
            return Err(MetaRaftError::new(
                "meta authentication",
                "distributed meta requires a credential file",
            ));
        }
    };
    let handle = MetaRaftHandle::new_durable_recovering_with_auth(
        config.raft.node_id,
        root,
        raft_config,
        incarnation.clone(),
        rpc_auth.clone(),
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
    let stored_replica = replica_store.identity().cloned();
    let claim = async {
        let peers: BTreeMap<_, _> = meta_peers
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
            for peer in &meta_peers {
                if let Ok(status) =
                    ursula_raft::meta_peer_recovery_status_authenticated(&peer.url, &rpc_auth).await
                {
                    statuses.insert(peer.node_id, status);
                }
            }
            let all_empty = statuses.len() == peers.len()
                && statuses
                    .values()
                    .all(|status| !status.initialized && status.vote.is_none());
            if initializer
                && (implicit_single
                    || config.raft.init_membership
                    || config.raft.init_membership_per_group)
                && all_empty
            {
                // Permits bind to each receiver's fresh boot. A delayed genesis
                // RPC cannot open a replacement process after a disk loss.
                for peer in &meta_peers {
                    let nonce = statuses
                        .get(&peer.node_id)
                        .and_then(|status| status.nonce.clone())
                        .ok_or_else(|| {
                            MetaRaftError::new("meta genesis", "peer lacks recovery nonce")
                        })?;
                    ursula_raft::meta_authorize_genesis_authenticated(&peer.url, &rpc_auth, nonce)
                        .await?;
                }
                handle.initialize_membership(peers.clone()).await?;
                break;
            }
            // A leader ReadIndex uses the current (possibly joint) membership.
            // Sampling a count from static bootstrap peers would be unsafe after
            // decommission, because a removed stale voter is no longer evidence.
            for peer in &meta_peers {
                if peer.node_id == config.raft.node_id {
                    continue;
                }
                if let Ok(floor) = ursula_raft::meta_peer_recovery_floor_authenticated(
                    &peer.url,
                    &rpc_auth,
                    config.raft.node_id,
                )
                .await
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
            if let (Some(stored), Some(ReplicaState::Retired(retired))) = (
                stored_replica.as_ref(),
                state.operations.replicas.get(&config.raft.node_id),
            ) && stored == retired
            {
                return Err(MetaRaftError::new(
                    "claim process boot",
                    "the stored replica identity is retired; its WAL cannot reclaim this node",
                ));
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
                    command: match (
                        stored_replica.as_ref(),
                        state.operations.processes.get(&config.raft.node_id),
                        state.operations.replicas.get(&config.raft.node_id),
                    ) {
                        (
                            Some(replica),
                            Some(ProcessState::Active(previous)),
                            Some(
                                ReplicaState::Active { identity, .. }
                                | ReplicaState::Pending {
                                    replacement: identity,
                                    ..
                                },
                            ),
                        ) if identity == replica => OperationCommand::RestartProcess {
                            node_id: config.raft.node_id,
                            previous: previous.clone(),
                            incarnation: incarnation.clone(),
                            replica: replica.clone(),
                        },
                        _ => OperationCommand::ClaimProcess {
                            node_id: config.raft.node_id,
                            expected_epoch,
                            incarnation: incarnation.clone(),
                        },
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
    let start = async {
        let process = claim.await?;
        let generation = process.epoch;
        let (replica_store, replica) = tokio::task::spawn_blocking(move || {
            let mut store = replica_store;
            let identity = store.bind_initial_generation(generation)?;
            Ok::<_, ursula_raft::wal::ReplicaIdentityError>((store, identity))
        })
        .await
        .map_err(|error| MetaRaftError::with_source("join replica identity bind", error))?
        .map_err(|error| MetaRaftError::with_source("bind replica identity", error))?;
        loop {
            let state = match handle.read_linearizable_state().await {
                Ok(state) => state,
                Err(error) => {
                    tracing::debug!(%error, "waiting for replica admission authority");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            match state.operations.replicas.get(&node_id) {
                Some(ReplicaState::Active { identity, .. }) if identity == &replica => {
                    // Initial genesis waits for every placement voter to publish
                    // its durable identity. Restarts reuse already registered
                    // identities and do not require those peers to be online.
                    let complete = state.placements.values().all(|placement| {
                        placement
                            .voters
                            .iter()
                            .all(|node| state.operations.replicas.contains_key(node))
                    });
                    if complete {
                        let identities = state
                            .operations
                            .replicas
                            .iter()
                            .map(|(node, state)| {
                                let identity = match state {
                                    ReplicaState::Active { identity, .. }
                                    | ReplicaState::Retired(identity) => identity,
                                    ReplicaState::Pending { previous, .. } => previous,
                                };
                                (*node, identity.clone())
                            })
                            .collect();
                        let mut prefixes = BTreeMap::<RaftGroupId, u64>::new();
                        for state in state.operations.replicas.values() {
                            if let ReplicaState::Active {
                                installed_groups, ..
                            } = state
                            {
                                for (group, index) in installed_groups {
                                    prefixes
                                        .entry(*group)
                                        .and_modify(|current| *current = (*current).max(*index))
                                        .or_insert(*index);
                                }
                            }
                        }
                        return Ok((process, replica, identities, prefixes, replica_store));
                    }
                }
                Some(ReplicaState::Pending { replacement, .. }) if replacement == &replica => {
                    // Survivors first durably install all group fences. Starting
                    // data actors earlier could let a blank disk count as the
                    // old voter; activation alone does not open its rejoin gate.
                }
                _ => {
                    match handle
                        .write(ControlCommand::Operation {
                            command: OperationCommand::RegisterReplica {
                                node_id,
                                process: process.clone(),
                                identity: replica.clone(),
                            },
                            now_ms: now_ms(),
                        })
                        .await
                    {
                        Ok(ControlResponse::Operation(Ok(OperationOutcome::ReplicaRegistered))) => {
                        }
                        Err(error) => {
                            tracing::debug!(%error, "checking whether replica registration committed")
                        }
                        Ok(response) => {
                            return Err(MetaRaftError::new(
                                "register replica identity",
                                format!("{response:?}"),
                            ));
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    match tokio::time::timeout(Duration::from_secs(120), start).await {
        Ok(Ok((process, replica, replica_genesis, replica_genesis_prefixes, replica_store))) => {
            Ok(MetaAuthority {
                handle,
                process,
                replica,
                replica_genesis,
                replica_genesis_prefixes,
                _replica_store: replica_store,
                shutdown,
                server,
            })
        }
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
