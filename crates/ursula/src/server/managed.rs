//! Managed adoption/startup. Bind meta first, then restore data assignments.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use openraft::BasicNode;
use openraft::Config;
use tokio::sync::Notify;
use tower::ServiceExt;
use ursula_config::UrsulaConfig;
use ursula_control::ClusterBootstrap;
use ursula_control::ControlProjection;
use ursula_control::ControlResponse;
use ursula_control::ProjectionCursor;
use ursula_raft::MetaGrpcRaftNetworkFactory;
use ursula_raft::MetaRaftGrpcService;
use ursula_raft::MetaRaftHandle;
use ursula_raft::meta_raft_grpc_service;
use ursula_raft::read_bootstrap_control_state;
use ursula_raft::read_meta_replica_status;

// Dropping an errored startup must close its bootstrap listener.
struct ListenerTask(tokio::task::JoinHandle<Result<(), std::io::Error>>);
impl Drop for ListenerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn run(
    config: &UrsulaConfig,
    maintenance_drained: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let control = config
        .control
        .as_ref()
        .ok_or_else(|| invalid("managed configuration is absent"))?;
    let recipe = control.bootstrap(config).map_err(invalid)?;
    let identity = control.local_identity(config).map_err(invalid)?;
    let listener = tokio::net::TcpListener::bind(
        config
            .server
            .cluster_listen
            .as_ref()
            .ok_or_else(|| invalid("cluster listener is absent"))?,
    )
    .await?;
    crate::bootstrap::check_and_stamp_format_epoch(config).await?;
    let raft_config = Arc::new(
        Config {
            cluster_name: format!("{}-meta", control.cluster_id.as_str()),
            heartbeat_interval: 250,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            max_in_snapshot_log_to_keep: 0,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                control.meta_snapshot_logs_since_last,
            ),
            ..Default::default()
        }
        .validate()?,
    );
    let meta = MetaRaftHandle::new_bound_durable_node_with_network(
        identity.clone(),
        raft_config,
        MetaGrpcRaftNetworkFactory::new_bound(identity.cluster.clone())?,
        &control.meta_journal_path,
    )
    .await?;
    // No admin listener or data-group initializer is exposed before adoption.
    // After startup the managed admin guard continues to reject raw mutations.
    let service = MetaRaftGrpcService::new_bound(&meta)?
        .with_managed_bootstrap(recipe.clone(), control.bootstrap_node_id)?;
    let slot = Arc::new(tokio::sync::RwLock::new(None::<Router>));
    let shutdown = Arc::new(Notify::new());
    let proxy_slot = slot.clone();
    let app = meta_router(service).fallback(move |request: Request<Body>| {
        let slot = proxy_slot.clone();
        async move {
            let router = slot.read().await.clone();
            match router {
                Some(router) => router
                    .oneshot(request)
                    .await
                    .expect("axum router is infallible"),
                None => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "managed data startup pending",
                )
                    .into_response(),
            }
        }
    });
    let mut cluster_task = ListenerTask(tokio::spawn(crate::server::serve_until_shutdown(
        listener,
        app,
        crate::server::notified(shutdown.clone()),
        None,
    )));
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let timeout = control.bootstrap_timeout.as_duration();
        let initial = tokio::time::timeout(timeout, async {
            loop {
                if cluster_task.0.is_finished() {
                    return Err(invalid("managed cluster listener stopped during bootstrap"));
                }
                if config.raft.node_id == control.bootstrap_node_id
                    && control.initialize_meta_membership
                    && !meta
                        .raft_handle()
                        .is_initialized()
                        .await
                        .map_err(|error| invalid(error.to_string()))?
                    && verify_bootstrap_peers(&recipe, control.bootstrap_node_id, true)
                        .await
                        .is_ok()
                {
                    let voters = recipe
                        .initial_meta_voters
                        .iter()
                        .filter_map(|id| {
                            recipe
                                .nodes
                                .get(id)
                                .map(|node| (*id, BasicNode::new(&node.cluster_url)))
                        })
                        .collect();
                    meta.initialize_membership(voters)
                        .await
                        .map_err(|error| invalid(error.to_string()))?;
                }
                if let Ok(state) = fetch_state(&recipe, false).await {
                    break Ok(state);
                }
                tokio::time::sleep(control.refresh_interval.as_duration()).await;
            }
        })
        .await??;
        if let Some(record) = &initial.state.cluster_bootstrap
            && record.recipe != recipe
        {
            return Err(invalid("configured bootstrap differs from persisted recipe").into());
        }
        let cursor = Arc::new(RwLock::new(
            ProjectionCursor::new(identity.cluster.clone()).map_err(invalid)?,
        ));
        // Data construction receives live placement if one exists. Neither a
        // restored nor a joining managed replica may initialize membership.
        let runtime_config = runtime_config(config, &initial)?;
        let state = super::init_state(&runtime_config, None, maintenance_drained)
            .await?
            .with_managed_projection(cursor.clone());
        *slot.write().await = Some(crate::cluster_router_from_state(state.clone()));
        let projection = if initial.state.cluster_bootstrap.is_some() {
            initial
        } else {
            tokio::time::timeout(timeout, async {
                loop {
                    let view = fetch_state(&recipe, false).await;
                    if let Ok(view) = view {
                        if view.state.cluster_bootstrap.is_some() {
                            break Ok::<_, std::io::Error>(view);
                        }
                        // Every initial data node must declare the same bound
                        // recipe and have raw membership administration closed.
                        if meta.raft_handle().is_leader()
                            && verify_bootstrap_peers(&recipe, control.bootstrap_node_id, false)
                                .await
                                .is_ok()
                        {
                            match meta
                                .bootstrap_cluster_from_quorums(
                                    recipe.clone(),
                                    Duration::from_secs(5),
                                    state.unix_time_ms(),
                                )
                                .await
                            {
                                Ok(ControlResponse::Ok) => {}
                                Ok(response) => {
                                    return Err(invalid(format!(
                                        "managed adoption rejected: {response}"
                                    )));
                                }
                                Err(error) => {
                                    tracing::debug!(%error, "waiting for bootstrap data quorums")
                                }
                            }
                        }
                    }
                    tokio::time::sleep(control.refresh_interval.as_duration()).await;
                }
            })
            .await??
        };
        cursor
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .install(projection)
            .map_err(invalid)?;
        state.register_otel_metrics();
        let client_listener = tokio::net::TcpListener::bind(&config.server.listen).await?;
        let admin_listener = tokio::net::TcpListener::bind(&config.server.admin_listen).await?;
        let refresh_recipe = recipe.clone();
        let refresh_cursor = cursor.clone();
        let interval = control.refresh_interval.as_duration();
        let refresh = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Ok(projection) = fetch_state(&refresh_recipe, true).await
                    && let Err(error) = refresh_cursor
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .install(projection)
                {
                    tracing::error!(%error, "reject managed projection refresh");
                }
            }
        });
        super::spawn_shutdown_signal_task(
            shutdown.clone(),
            state.raft_registry().cloned(),
            config.raft.node_id,
            runtime_config
                .raft
                .peers
                .iter()
                .map(|peer| (peer.node_id, peer.url.clone()))
                .collect(),
        );

        let client_app = crate::client_router_with_admission(
            state.clone(),
            crate::IngressAdmission::new(&config.server)
                .with_wal_disk_monitor(state.wal_disk_monitor())
                .with_raft_log_pressure(
                    state
                        .raft_registry()
                        .map(ursula_raft::RaftGroupHandleRegistry::snapshot_build_coordinator),
                ),
        );
        let client = super::serve_until_shutdown(
            client_listener,
            client_app,
            super::notified(shutdown.clone()),
            None,
        );
        let admin = super::serve_until_shutdown(
            admin_listener,
            crate::admin_router(state),
            super::notified(shutdown.clone()),
            None,
        );
        let cluster = async { (&mut cluster_task.0).await.map_err(std::io::Error::other)? };
        let result = tokio::try_join!(client, admin, cluster);
        refresh.abort();
        result?;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    meta.shutdown().await?;
    result
}

fn invalid(reason: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, reason.into())
}

fn meta_router(service: MetaRaftGrpcService) -> Router {
    let mut router = Router::new();
    for path in [
        ursula_raft::META_RAFT_APPEND_PATH,
        ursula_raft::META_RAFT_VOTE_PATH,
        ursula_raft::META_RAFT_FULL_SNAPSHOT_PATH,
        ursula_raft::META_RAFT_TRANSFER_LEADER_PATH,
        ursula_raft::META_RAFT_READ_PROJECTION_PATH,
        ursula_raft::META_RAFT_READ_BOOTSTRAP_STATE_PATH,
        ursula_raft::META_RAFT_STATUS_PATH,
    ] {
        router = router.route_service(path, meta_raft_grpc_service(service.clone()));
    }
    router
}

async fn verify_bootstrap_peers(
    recipe: &ClusterBootstrap,
    bootstrap_node_id: u64,
    require_empty_meta: bool,
) -> Result<(), std::io::Error> {
    for node in recipe.nodes.values() {
        let status = read_meta_replica_status(
            &recipe.identity,
            node.node_id,
            &node.cluster_url,
            Duration::from_secs(1),
        )
        .await
        .map_err(|error| invalid(error.to_string()))?;
        validate_bootstrap_peer(recipe, bootstrap_node_id, node, &status, require_empty_meta)?;
    }
    Ok(())
}

fn validate_bootstrap_peer(
    recipe: &ClusterBootstrap,
    bootstrap_node_id: u64,
    node: &ursula_control::NodeRegistration,
    status: &ursula_raft::MetaReplicaStatus,
    require_empty_meta: bool,
) -> Result<(), std::io::Error> {
    if require_empty_meta
        && recipe.initial_meta_voters.contains(&node.node_id)
        && status.initialized
    {
        return Err(invalid(
            "refuse meta initialization beside an established peer; wait for rejoin",
        ));
    }
    if status.identity.node != *node
        || status.bootstrap_recipe.as_ref() != Some(recipe)
        || status.bootstrap_node_id != Some(bootstrap_node_id)
    {
        return Err(invalid(format!(
            "node {} does not declare the trusted managed bootstrap",
            node.node_id
        )));
    }
    Ok(())
}

async fn fetch_state(
    recipe: &ClusterBootstrap,
    complete: bool,
) -> Result<ControlProjection, std::io::Error> {
    for id in &recipe.initial_meta_voters {
        let node = recipe
            .nodes
            .get(id)
            .ok_or_else(|| invalid("meta voter lacks trusted origin"))?;
        let view = read_bootstrap_control_state(
            &recipe.identity,
            *id,
            &node.cluster_url,
            Duration::from_secs(1),
        )
        .await;
        if let Ok(view) = view {
            if complete {
                view.validate().map_err(invalid)?;
            }
            return Ok(view);
        }
    }
    Err(invalid("no fresh meta quorum state is available"))
}

fn runtime_config(
    config: &UrsulaConfig,
    projection: &ControlProjection,
) -> Result<UrsulaConfig, std::io::Error> {
    let mut runtime = config.clone();
    runtime.control = None;
    runtime.raft.init_membership = false;
    runtime.raft.init_membership_per_group = false;
    if projection.state.cluster_bootstrap.is_some() {
        projection.validate().map_err(invalid)?;
        if projection
            .state
            .nodes
            .get(&config.raft.node_id)
            .is_some_and(|node| {
                matches!(
                    node.state,
                    ursula_control::NodeState::Disabled | ursula_control::NodeState::Removed
                )
            })
        {
            return Err(invalid(
                "managed node is disabled or removed; refuse to recreate its actors",
            ));
        }
        let mut peers = projection
            .state
            .nodes
            .iter()
            .filter(|(_, node)| node.state != ursula_control::NodeState::Removed)
            .map(|(id, node)| (*id, node.cluster_url.clone()))
            .collect::<BTreeMap<_, _>>();
        if let Some(control) = &config.control {
            peers.insert(
                config.raft.node_id,
                control
                    .node
                    .clone()
                    .normalize()
                    .map_err(invalid)?
                    .cluster_url,
            );
        }
        runtime.raft.peers = peers
            .into_iter()
            .map(|(node_id, url)| ursula_config::RaftPeerConfig { node_id, url })
            .collect();
        runtime.raft.groups = projection
            .state
            .placements
            .iter()
            .map(|(group, placement)| ursula_config::RaftGroupConfig {
                raft_group_id: group.0,
                voters: placement.voters.iter().copied().collect(),
            })
            .collect();
    }
    Ok(runtime)
}

#[cfg(test)]
mod tests {
    use ursula_control::ControlCommand;
    use ursula_control::ControlPlaneState;
    use ursula_control::MembershipLogId;
    use ursula_control::VerifiedGroupMembership;
    use ursula_shard::RaftGroupId;

    use super::*;

    fn bootstrap_recipe() -> ClusterBootstrap {
        let nodes = (1..=5)
            .map(|id| {
                (id, ursula_control::NodeRegistration {
                    node_id: id,
                    client_url: format!("http://node{id}:4437"),
                    cluster_url: format!("http://node{id}:4440"),
                    admin_url: format!("http://node{id}:4438"),
                    labels: BTreeMap::from([("zone".to_owned(), ((id - 1) % 3).to_string())]),
                })
            })
            .collect::<BTreeMap<_, _>>();
        ClusterBootstrap {
            identity: ursula_control::ClusterIdentity {
                cluster_id: ursula_control::ClusterId::try_from("restoration-test".to_owned())
                    .unwrap(),
                group_count: 1,
                core_count: 1,
                routing_hash: ursula_control::RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
            },
            initial_meta_voters: [1, 2, 3].into(),
            nodes: nodes.clone(),
            voters: BTreeMap::from([(RaftGroupId(0), [1, 2, 3].into())]),
            placement: Default::default(),
        }
    }

    #[test]
    fn bootstrap_cohort_requires_one_coordinator_and_no_existing_meta_membership() {
        let recipe = bootstrap_recipe();
        let node = &recipe.nodes[&2];
        let mut status = ursula_raft::MetaReplicaStatus {
            identity: ursula_control::MetaLocalIdentity {
                cluster: recipe.identity.clone(),
                node: node.clone(),
            },
            initialized: false,
            current_leader: None,
            snapshot_index: None,
            purged_log_index: None,
            bootstrap_recipe: Some(recipe.clone()),
            bootstrap_node_id: Some(1),
        };
        assert!(validate_bootstrap_peer(&recipe, 1, node, &status, true).is_ok());
        status.bootstrap_node_id = Some(2);
        assert!(validate_bootstrap_peer(&recipe, 1, node, &status, true).is_err());
        status.bootstrap_node_id = None;
        assert!(validate_bootstrap_peer(&recipe, 1, node, &status, true).is_err());
        status.bootstrap_node_id = Some(1);
        status.bootstrap_recipe = None;
        assert!(validate_bootstrap_peer(&recipe, 1, node, &status, true).is_err());
        status.bootstrap_recipe = Some(recipe.clone());
        status.initialized = true;
        assert!(validate_bootstrap_peer(&recipe, 1, node, &status, true).is_err());
        assert!(validate_bootstrap_peer(&recipe, 1, node, &status, false).is_ok());
    }

    #[test]
    fn runtime_restoration_uses_current_projection_and_never_initializes_data_membership() {
        let recipe = bootstrap_recipe();
        let nodes = recipe.nodes.clone();
        let mut state = ControlPlaneState::default();
        assert_eq!(
            state.apply(ControlCommand::BootstrapCluster {
                bootstrap: recipe.clone(),
                memberships: BTreeMap::from([(RaftGroupId(0), VerifiedGroupMembership {
                    voters: [1, 2, 3].into(),
                    learners: Default::default(),
                    log_id: MembershipLogId {
                        term: 1,
                        node_id: 1,
                        index: 1
                    }
                })]),
                now_ms: 1
            }),
            ControlResponse::Ok
        );
        // The metadata truth has advanced past the immutable bootstrap recipe.
        // This fixture tests projection consumption, not a real Raft migration.
        state.placements.get_mut(&RaftGroupId(0)).unwrap().voters = [2, 3, 4].into();
        state.placements.get_mut(&RaftGroupId(0)).unwrap().epoch = 1;
        let mut view = ControlProjection {
            identity: recipe.identity.clone(),
            applied_log_id: MembershipLogId {
                term: 2,
                node_id: 2,
                index: 100,
            },
            state,
        };
        view.validate().unwrap();
        let mut config = UrsulaConfig::default();
        config.raft.node_id = 1;
        config.raft.group_count = 1;
        config.raft.init_membership = true;
        config.raft.init_membership_per_group = true;
        config.raft.groups = vec![ursula_config::RaftGroupConfig {
            raft_group_id: 0,
            voters: vec![1, 2, 3],
        }];
        let restored = runtime_config(&config, &view).unwrap();
        assert_eq!(restored.raft.groups[0].voters, vec![2, 3, 4]);
        assert!(!restored.raft.init_membership);
        assert!(!restored.raft.init_membership_per_group);
        assert_eq!(restored.raft.peers[0].url, nodes[&1].cluster_url);
        view.state.nodes.get_mut(&1).unwrap().state = ursula_control::NodeState::Removed;
        assert!(runtime_config(&config, &view).is_err());
    }
}
