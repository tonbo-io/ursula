//! Full-snapshot routing hints from the independently replicated meta quorum.
//! Cached hints select a front door; data Raft remains the serving authority.

use std::sync::Mutex;
use std::time::Duration;

use axum::http::Uri;
use tokio::time::Instant;
use ursula_control::ClusterBootstrap;
use ursula_control::ControlProjection;
use ursula_control::NodeState;
use ursula_control::ProjectionCursor;
use ursula_control::ProjectionInstall;
use ursula_shard::RaftGroupId;
use ursula_shard::StaticShardMap;

const READ_TIMEOUT: Duration = Duration::from_secs(2);
const REFRESH_COOLDOWN: Duration = Duration::from_millis(100);

pub(crate) struct ManagedDirectory {
    bootstrap: ClusterBootstrap,
    cursor: Mutex<ProjectionCursor>,
    refresh: tokio::sync::Mutex<Option<Instant>>,
}

impl ManagedDirectory {
    pub(crate) fn new(bootstrap: ClusterBootstrap) -> Result<Self, String> {
        if bootstrap.initial_meta_voters.iter().any(|id| {
            bootstrap
                .nodes
                .get(id)
                .is_none_or(|node| !node.cluster_url.starts_with("http://"))
        }) {
            return Err(
                "gateway meta RPC requires HTTP origins; TLS transport is not configured"
                    .to_owned(),
            );
        }
        Ok(Self {
            cursor: Mutex::new(ProjectionCursor::new(bootstrap.identity.clone())?),
            bootstrap,
            refresh: tokio::sync::Mutex::new(None),
        })
    }

    /// Serialize refresh and cool down failed as well as successful attempts.
    /// The lock spans only bounded RPC I/O and is never needed to read hints.
    pub(crate) async fn refresh(&self) -> Result<bool, String> {
        let mut last = self.refresh.lock().await;
        if last.is_some_and(|last| last.elapsed() < REFRESH_COOLDOWN) {
            return if self
                .cursor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .current()
                .is_some()
            {
                Ok(false)
            } else {
                Err("no installed managed routing directory; refresh is cooling down".to_owned())
            };
        }
        let result = self.read().await.and_then(|view| self.install(view));
        *last = Some(Instant::now());
        result
    }

    async fn read(&self) -> Result<ControlProjection, String> {
        let mut reason = "no reachable bound meta leader".to_owned();
        // Never follow a leader URL supplied by an unavailable/untrusted peer.
        // Meta voter changes are a separate protocol, not data registration.
        for id in &self.bootstrap.initial_meta_voters {
            let node = self
                .bootstrap
                .nodes
                .get(id)
                .ok_or("bootstrap lacks meta origin")?;
            match ursula_raft::read_control_projection(
                &self.bootstrap.identity,
                *id,
                &node.cluster_url,
                READ_TIMEOUT,
            )
            .await
            {
                Ok(view) => return Ok(view),
                Err(error) => reason = error.to_string(),
            }
        }
        Err(reason)
    }

    fn install(&self, view: ControlProjection) -> Result<bool, String> {
        if view
            .state
            .cluster_bootstrap
            .as_ref()
            .map(|record| &record.recipe)
            != Some(&self.bootstrap)
        {
            return Err(
                "gateway projection differs from its immutable bootstrap recipe".to_owned(),
            );
        }
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(cursor.install(view)? == ProjectionInstall::Advanced)
    }

    pub(crate) fn leader_origin(&self, id: u64, origin: &str) -> Option<String> {
        let cursor = self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let node = cursor.current()?.state.nodes.get(&id)?;
        (serving(node.state) && node.client_url == origin).then(|| node.client_url.clone())
    }

    pub(crate) fn is_serving_origin(&self, origin: &str) -> bool {
        let cursor = self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cursor.current().is_some_and(|view| {
            view.state
                .nodes
                .values()
                .any(|node| serving(node.state) && node.client_url == origin)
        })
    }

    pub(crate) fn upstreams(&self, uri: &Uri, map: Option<&StaticShardMap>) -> Vec<String> {
        let group = super::upstream_pin_key(uri, map).and_then(|key| {
            key.strip_prefix("group:")
                .and_then(|id| id.parse::<u32>().ok())
                .map(RaftGroupId)
        });
        let cursor = self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(view) = cursor.current() else {
            return Vec::new();
        };
        view.state
            .nodes
            .values()
            .filter(|node| {
                serving(node.state)
                    && group.is_none_or(|group| {
                        view.state
                            .placements
                            .get(&group)
                            .is_some_and(|placement| placement.voters.contains(&node.node_id))
                    })
            })
            .map(|node| node.client_url.clone())
            .collect()
    }
}

fn serving(state: NodeState) -> bool {
    matches!(state, NodeState::Active | NodeState::Draining)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use ursula_control::ClusterId;
    use ursula_control::ClusterIdentity;
    use ursula_control::ControlCommand;
    use ursula_control::ControlPlaneState;
    use ursula_control::ControlResponse;
    use ursula_control::MembershipLogId;
    use ursula_control::NodeRegistration;
    use ursula_control::RoutingHashVersion;
    use ursula_control::VerifiedGroupMembership;

    use super::*;

    fn fixture() -> (ClusterBootstrap, ControlProjection) {
        let bootstrap = ClusterBootstrap {
            identity: ClusterIdentity {
                cluster_id: ClusterId::try_from("gateway-managed".to_owned()).unwrap(),
                group_count: 1,
                core_count: 1,
                routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
            },
            initial_meta_voters: BTreeSet::from([1, 2, 3]),
            nodes: (1..=3)
                .map(|id| {
                    (id, NodeRegistration {
                        node_id: id,
                        client_url: format!("http://node{id}:4437"),
                        cluster_url: format!("http://node{id}:4440"),
                        admin_url: format!("http://node{id}:4438"),
                        labels: BTreeMap::from([("zone".to_owned(), id.to_string())]),
                    })
                })
                .collect(),
            voters: BTreeMap::from([(RaftGroupId(0), BTreeSet::from([1, 2, 3]))]),
            placement: Default::default(),
        }
        .normalize()
        .unwrap();
        let log = MembershipLogId {
            term: 1,
            node_id: 1,
            index: 1,
        };
        let mut state = ControlPlaneState::default();
        assert_eq!(
            state.apply(ControlCommand::BootstrapCluster {
                bootstrap: bootstrap.clone(),
                memberships: BTreeMap::from([(RaftGroupId(0), VerifiedGroupMembership {
                    voters: BTreeSet::from([1, 2, 3]),
                    learners: BTreeSet::new(),
                    log_id: log.clone(),
                })]),
                now_ms: 1,
            }),
            ControlResponse::Ok
        );
        let view = ControlProjection {
            identity: bootstrap.identity.clone(),
            state,
            applied_log_id: log,
        };
        (bootstrap, view)
    }

    #[test]
    fn complete_ordered_directory_rejects_rollback_conflict_and_bootstrap_drift() {
        let (bootstrap, view) = fixture();
        let directory = ManagedDirectory::new(bootstrap).unwrap();
        let uri = "/bucket/stream".parse().unwrap();
        let map = StaticShardMap::new(1, 1).unwrap();
        assert!(directory.upstreams(&uri, Some(&map)).is_empty());
        assert!(directory.install(view.clone()).unwrap());
        let mut next = view.clone();
        next.applied_log_id.index = 2;
        next.state.apply(ControlCommand::SetNodeState {
            node_id: 1,
            state: NodeState::Disabled,
            now_ms: 2,
        });
        assert!(directory.install(next.clone()).unwrap());
        assert!(!directory.install(view.clone()).unwrap());
        assert!(!directory.is_serving_origin("http://node1:4437"));
        assert_eq!(directory.upstreams(&uri, Some(&map)).len(), 2);
        let mut conflict = next.clone();
        conflict.state.nodes.get_mut(&2).unwrap().updated_at_ms = 99;
        let _conflict = directory.install(conflict).unwrap_err();
        assert!(!directory.install(next.clone()).unwrap());
        next.state
            .cluster_bootstrap
            .as_mut()
            .unwrap()
            .recipe
            .nodes
            .get_mut(&1)
            .unwrap()
            .admin_url = "http://other:4438".to_owned();
        let _drift = directory.install(next).unwrap_err();
        assert_eq!(
            directory.leader_origin(2, "http://node2:4437"),
            Some("http://node2:4437".to_owned())
        );
        assert!(directory.leader_origin(2, "http://node2:44370").is_none());
        assert!(directory.leader_origin(3, "http://node2:4437").is_none());
        assert!(directory.leader_origin(4, "http://node4:4437").is_none());
        assert!(directory.leader_origin(1, "http://node1:4437").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_cooldown_cannot_certify_an_empty_directory() {
        let (bootstrap, view) = fixture();
        let directory = ManagedDirectory::new(bootstrap).unwrap();
        *directory.refresh.lock().await = Some(Instant::now());
        let _missing = directory.refresh().await.unwrap_err();
        assert!(directory.install(view).unwrap());
        assert!(!directory.refresh().await.unwrap());
    }

    #[tokio::test]
    async fn unknown_managed_redirect_fails_closed_and_retains_existing_hints() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use axum::Router;
        use axum::body::Body;
        use axum::http::Request;
        use axum::http::StatusCode;
        use axum::routing::any;

        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let unknown = crate::tests::spawn_upstream(Router::new().route(
            "/bucket/stream",
            any(move || {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    "unexpected forwarding"
                }
            }),
        ))
        .await;
        let location = format!("{}/bucket/stream", unknown.url);
        let follower = crate::tests::spawn_upstream(Router::new().route(
            "/bucket/stream",
            any(move || {
                let location = location.clone();
                async move {
                    (StatusCode::TEMPORARY_REDIRECT, [
                        ("location", location),
                        ("x-ursula-raft-leader-id", "4".to_owned()),
                    ])
                }
            }),
        ))
        .await;
        let (mut bootstrap, mut view) = fixture();
        // Mock HTTP front doors cannot establish meta authority. Closed ports
        // make the remaining bound meta reads fail promptly without DNS.
        let mut listeners = Vec::new();
        for id in [2, 3] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            bootstrap.nodes.get_mut(&id).unwrap().cluster_url =
                format!("http://{}", listener.local_addr().unwrap());
            listeners.push(listener);
        }
        drop(listeners);
        bootstrap.nodes.get_mut(&1).unwrap().client_url = follower.url.clone();
        bootstrap.nodes.get_mut(&1).unwrap().cluster_url = follower.url.clone();
        for (id, node) in &bootstrap.nodes {
            let installed = view.state.nodes.get_mut(id).unwrap();
            installed.client_url = node.client_url.clone();
            installed.cluster_url = node.cluster_url.clone();
        }
        view.state.cluster_bootstrap.as_mut().unwrap().recipe = bootstrap.clone();
        let gateway = crate::Gateway::new(crate::tests::test_config(Vec::new()))
            .with_managed_directory(bootstrap)
            .unwrap();
        let directory = gateway.managed.as_ref().unwrap();
        directory.install(view).unwrap();
        let (parts, _) = Request::builder()
            .uri("/bucket/stream")
            .body(Body::empty())
            .unwrap()
            .into_parts();
        let response = gateway
            .forward(
                &follower.url,
                &parts,
                bytes::Bytes::new(),
                crate::ResponseTail::default(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        assert!(!response.headers().contains_key("location"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            directory.leader_origin(1, &follower.url),
            Some(follower.url.clone())
        );
    }
}
