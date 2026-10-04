//! Replicated group feature levels (design C0).
//!
//! `ursulactl cluster enable-feature --level N` raises every Raft group's
//! replicated feature level to `N` in three steps:
//!
//! 1. **Support check.** Every voter and learner of every group (as reported
//!    by `/__ursula/metrics`), and every manifest node, must report through
//!    `GET /__ursula/feature-level` a `supported_level >= N`. A node that is
//!    unreachable, predates the endpoint (404), or is a group member missing
//!    from the manifest fails the check, and nothing is proposed.
//! 2. **Propose.** `POST /__ursula/feature-level {"level": N}` on every node.
//!    Each node proposes `SetFeatureLevel` for the groups it leads; groups
//!    led elsewhere come back as `not_leader` and are covered by their own
//!    leader's call. Apply is `max(current, N)`, so repeats are harmless.
//! 3. **Verify.** Re-read every node until every hosted group replica reports
//!    a level `>= N`, re-proposing while some group lags (a leadership change
//!    can race the first pass), up to the timeout.
//!
//! The decision logic is pure ([`check_feature_support`],
//! [`verify_feature_levels`]) so it is tested against fake cluster views.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::metrics::ClusterSnapshot;
use crate::metrics::MetricsClient;
use crate::provider::NodeInfo;

/// One node's answer to `GET /__ursula/feature-level`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FeatureLevelReport {
    #[serde(default)]
    pub node_id: Option<u64>,
    pub supported_level: u32,
    #[serde(default)]
    pub groups: Vec<GroupFeatureLevel>,
}

/// One group replica's level as held by the reporting node.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct GroupFeatureLevel {
    pub raft_group_id: u64,
    #[serde(default = "default_hosted")]
    pub hosted: bool,
    #[serde(default)]
    pub level: Option<u32>,
    #[serde(default)]
    pub error: Option<String>,
    /// Whether the reporting node completed a cold-index page-repair cycle
    /// as this group's leader (bounded-state F19). Absent on nodes that
    /// predate level 2, which counts as not completed.
    #[serde(default)]
    pub page_repair_completed: bool,
}

/// First feature level whose raise needs a completed page-repair cycle in
/// every group (bounded-state Lb2, F1 sparse marks).
pub const PAGE_REPAIR_REQUIRED_LEVEL: u32 = 2;

fn default_hosted() -> bool {
    true
}

/// One node's answer to `POST /__ursula/feature-level`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SetFeatureLevelReport {
    #[serde(default)]
    pub groups: Vec<GroupFeatureLevelOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct GroupFeatureLevelOutcome {
    pub raft_group_id: u64,
    /// `set`, `not_leader`, `not_hosted`, `repair_pending` (level 2 and up,
    /// no completed page-repair cycle on this node), or `error`.
    pub status: String,
    #[serde(default)]
    pub level: Option<u32>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnableFeatureOptions {
    pub level: u32,
    pub timeout: Duration,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnableFeatureOutcome {
    pub level: u32,
    /// Groups verified at `>= level` on every replica that hosts them.
    pub groups: usize,
    /// Nodes whose replicas were verified.
    pub nodes: usize,
}

impl MetricsClient {
    /// `GET /__ursula/feature-level`. `Ok(None)` when the node predates
    /// feature levels (404), which callers must treat as "supports level 0".
    pub async fn feature_level_report(
        &self,
        node: &NodeInfo,
    ) -> Result<Option<FeatureLevelReport>> {
        let url = node
            .admin_url
            .join("/__ursula/feature-level")
            .with_context(|| format!("compose feature-level url for node {}", node.id))?;
        let resp = self
            .http()
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "feature-level report from node {} returned {status}: {body}",
                node.id
            );
        }
        serde_json::from_str(&body)
            .map(Some)
            .with_context(|| format!("decode feature-level report from node {}: {body}", node.id))
    }

    /// `POST /__ursula/feature-level {"level": N}`.
    pub async fn set_feature_level(
        &self,
        node: &NodeInfo,
        level: u32,
    ) -> Result<SetFeatureLevelReport> {
        let url = node
            .admin_url
            .join("/__ursula/feature-level")
            .with_context(|| format!("compose feature-level url for node {}", node.id))?;
        let resp = self
            .http()
            .post(url.clone())
            .json(&serde_json::json!({ "level": level }))
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "set feature level {level} at node {} returned {status}: {body}",
                node.id
            );
        }
        serde_json::from_str(&body).with_context(|| {
            format!(
                "decode set-feature-level response from node {}: {body}",
                node.id
            )
        })
    }
}

/// Every node id that must support a level before it is raised: each voter
/// and learner any node reports for any group, plus every manifest node.
pub fn cluster_member_ids(snapshot: &ClusterSnapshot, nodes: &[NodeInfo]) -> BTreeSet<u64> {
    let mut members = nodes.iter().map(|node| node.id).collect::<BTreeSet<_>>();
    for view in &snapshot.per_node {
        for group in &view.groups {
            members.extend(group.voter_ids.iter().copied());
            members.extend(group.learner_ids.iter().copied());
        }
    }
    members
}

/// Support check: `Ok` only when every member reported a supported level
/// `>= level`. `supported` maps node id to the level it reported; a member
/// absent from the map (unreachable, unknown to the manifest) fails, and a
/// legacy node without the endpoint is reported as level 0 by the caller.
pub fn check_feature_support(
    level: u32,
    members: &BTreeSet<u64>,
    supported: &BTreeMap<u64, u32>,
) -> Result<()> {
    let mut problems = Vec::new();
    for member in members {
        match supported.get(member) {
            Some(node_level) if *node_level >= level => {}
            Some(node_level) => problems.push(format!(
                "node {member} supports feature level {node_level}, below {level}"
            )),
            None => problems.push(format!(
                "node {member} did not report a supported feature level"
            )),
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "refusing to enable feature level {level}: {}",
            problems.join("; ")
        ))
    }
}

/// Page-repair check for the raise to level 2 (bounded-state §5.1): every
/// group any node reports must have a node reporting a completed cold-index
/// page-repair cycle for it. `Ok` below level 2.
pub fn check_page_repair(level: u32, reports: &BTreeMap<u64, FeatureLevelReport>) -> Result<()> {
    if level < PAGE_REPAIR_REQUIRED_LEVEL {
        return Ok(());
    }
    let mut groups = BTreeMap::<u64, bool>::new();
    for report in reports.values() {
        for group in &report.groups {
            let completed = groups.entry(group.raft_group_id).or_default();
            *completed = *completed || (group.hosted && group.page_repair_completed);
        }
    }
    let pending = groups
        .into_iter()
        .filter(|(_, completed)| !completed)
        .map(|(group, _)| group.to_string())
        .collect::<Vec<_>>();
    if pending.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "refusing to enable feature level {level}: no node reports a completed cold-index \
             page-repair cycle for group(s) {}",
            pending.join(", ")
        ))
    }
}

/// Verification: every group replica each node hosts reports a level
/// `>= level`, and every group id seen anywhere is hosted somewhere. Returns
/// the number of verified groups, or one line per lagging replica.
pub fn verify_feature_levels(
    level: u32,
    reports: &BTreeMap<u64, FeatureLevelReport>,
) -> std::result::Result<usize, Vec<String>> {
    let mut lagging = Vec::new();
    let mut all_groups = BTreeSet::new();
    let mut hosted_groups = BTreeSet::new();
    for (node_id, report) in reports {
        for group in &report.groups {
            all_groups.insert(group.raft_group_id);
            if !group.hosted {
                continue;
            }
            hosted_groups.insert(group.raft_group_id);
            match (group.level, group.error.as_deref()) {
                (Some(current), _) if current >= level => {}
                (Some(current), _) => lagging.push(format!(
                    "node {node_id} group {}: level {current}",
                    group.raft_group_id
                )),
                (None, error) => lagging.push(format!(
                    "node {node_id} group {}: {}",
                    group.raft_group_id,
                    error.unwrap_or("no level reported")
                )),
            }
        }
    }
    for group in all_groups.difference(&hosted_groups) {
        lagging.push(format!("group {group}: no node hosts it"));
    }
    if lagging.is_empty() {
        Ok(all_groups.len())
    } else {
        Err(lagging)
    }
}

/// Every manifest node's feature-level report. A node without the endpoint
/// reports `supported_level = 0` and no groups.
async fn collect_reports(
    client: &MetricsClient,
    nodes: &[NodeInfo],
) -> Result<BTreeMap<u64, FeatureLevelReport>> {
    let mut reports = BTreeMap::new();
    for node in nodes {
        let report = client
            .feature_level_report(node)
            .await?
            .unwrap_or(FeatureLevelReport {
                node_id: Some(node.id),
                supported_level: 0,
                groups: Vec::new(),
            });
        reports.insert(node.id, report);
    }
    Ok(reports)
}

/// `ursulactl cluster enable-feature`: support check, propose, verify. See
/// the module docs.
pub async fn enable_feature(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    options: &EnableFeatureOptions,
) -> Result<EnableFeatureOutcome> {
    let level = options.level;
    let snapshot = client
        .fetch_cluster(nodes)
        .await
        .context("read cluster membership before enabling a feature level")?;
    let members = cluster_member_ids(&snapshot, nodes);
    let reports = collect_reports(client, nodes).await?;
    let supported = reports
        .iter()
        .map(|(node_id, report)| (*node_id, report.supported_level))
        .collect::<BTreeMap<_, _>>();
    check_feature_support(level, &members, &supported)?;
    check_page_repair(level, &reports)?;

    let deadline = tokio::time::Instant::now() + options.timeout;
    loop {
        for node in nodes {
            let report = client.set_feature_level(node, level).await?;
            for group in &report.groups {
                if group.status == "repair_pending" {
                    tracing::info!(
                        node_id = node.id,
                        raft_group_id = group.raft_group_id,
                        "node has no completed page-repair cycle for group; its leader proposes"
                    );
                }
                if group.status == "error" {
                    tracing::warn!(
                        node_id = node.id,
                        raft_group_id = group.raft_group_id,
                        error = group.error.as_deref().unwrap_or_default(),
                        "set feature level failed for group; will retry"
                    );
                }
            }
        }
        let reports = collect_reports(client, nodes).await?;
        match verify_feature_levels(level, &reports) {
            Ok(groups) => {
                return Ok(EnableFeatureOutcome {
                    level,
                    groups,
                    nodes: reports.len(),
                });
            }
            Err(lagging) => {
                if tokio::time::Instant::now() >= deadline {
                    bail!(
                        "feature level {level} not verified on every replica before timeout: {}",
                        lagging.join("; ")
                    );
                }
                tracing::info!(
                    lagging = lagging.len(),
                    "feature level not yet applied everywhere; retrying"
                );
            }
        }
        tokio::time::sleep(options.poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use axum::Json;
    use axum::Router;
    use axum::extract::State;
    use axum::routing::get;
    use url::Url;

    use super::*;
    use crate::metrics::NodeMetricsView;
    use crate::metrics::RaftGroupView;

    fn node(id: u64) -> NodeInfo {
        NodeInfo {
            id,
            admin_url: Url::parse(&format!("http://127.0.0.1:{}", 4000 + id)).unwrap(),
            host: "127.0.0.1".to_owned(),
            http_url: None,
        }
    }

    fn group_view(
        raft_group_id: u64,
        node_id: u64,
        voters: &[u64],
        learners: &[u64],
    ) -> RaftGroupView {
        RaftGroupView {
            raft_group_id,
            node_id,
            current_term: Some(1),
            current_leader: voters.first().copied(),
            committed_index: Some(1),
            last_applied_index: Some(1),
            voter_ids: voters.to_vec(),
            learner_ids: learners.to_vec(),
        }
    }

    fn report(supported_level: u32, groups: &[(u64, Option<u32>)]) -> FeatureLevelReport {
        FeatureLevelReport {
            node_id: None,
            supported_level,
            groups: groups
                .iter()
                .map(|(raft_group_id, level)| GroupFeatureLevel {
                    raft_group_id: *raft_group_id,
                    hosted: true,
                    level: *level,
                    error: None,
                    page_repair_completed: false,
                })
                .collect(),
        }
    }

    #[test]
    fn members_include_learners_outside_the_manifest() {
        let snapshot = ClusterSnapshot {
            per_node: vec![NodeMetricsView {
                node: node(1),
                groups: vec![group_view(0, 1, &[1, 2], &[7])],
                wal_backend: None,
            }],
        };
        let members = cluster_member_ids(&snapshot, &[node(1), node(2), node(3)]);
        assert_eq!(members, BTreeSet::from([1, 2, 3, 7]));
    }

    #[test]
    fn support_check_refuses_old_or_unreported_members() {
        let members = BTreeSet::from([1, 2, 3]);
        let all_new = BTreeMap::from([(1, 1), (2, 1), (3, 2)]);
        check_feature_support(1, &members, &all_new).unwrap();

        let one_old = BTreeMap::from([(1, 1), (2, 0), (3, 1)]);
        let err = check_feature_support(1, &members, &one_old).unwrap_err();
        assert!(err.to_string().contains("node 2 supports feature level 0"));

        let learner_missing = BTreeMap::from([(1, 1), (2, 1)]);
        let err = check_feature_support(1, &members, &learner_missing).unwrap_err();
        assert!(err.to_string().contains("node 3 did not report"));
    }

    #[test]
    fn verification_requires_every_hosted_replica_at_level() {
        let done = BTreeMap::from([
            (1, report(1, &[(0, Some(1)), (1, Some(2))])),
            (2, report(1, &[(0, Some(1)), (1, Some(1))])),
        ]);
        assert_eq!(verify_feature_levels(1, &done), Ok(2));

        let lagging = BTreeMap::from([
            (1, report(1, &[(0, Some(1)), (1, Some(1))])),
            (2, report(1, &[(0, Some(0)), (1, None)])),
        ]);
        let lines = verify_feature_levels(1, &lagging).unwrap_err();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("node 2 group 0: level 0"));
    }

    #[test]
    fn verification_ignores_unhosted_replicas_but_not_orphan_groups() {
        let mut node_two = report(1, &[(0, Some(1))]);
        node_two.groups.push(GroupFeatureLevel {
            raft_group_id: 1,
            hosted: false,
            level: None,
            error: None,
            page_repair_completed: false,
        });
        let reports =
            BTreeMap::from([(1, report(1, &[(0, Some(1)), (1, Some(1))])), (2, node_two)]);
        assert_eq!(verify_feature_levels(1, &reports), Ok(2));

        let orphan = BTreeMap::from([(1, FeatureLevelReport {
            node_id: Some(1),
            supported_level: 1,
            groups: vec![GroupFeatureLevel {
                raft_group_id: 3,
                hosted: false,
                level: None,
                error: None,
                page_repair_completed: false,
            }],
        })]);
        assert!(verify_feature_levels(1, &orphan).is_err());
    }

    #[test]
    fn page_repair_check_gates_only_level_two_and_up() {
        let mut leader = report(2, &[(0, Some(1)), (1, Some(1))]);
        leader.groups[0].page_repair_completed = true;
        let follower = report(2, &[(0, Some(1)), (1, Some(1))]);
        let reports = BTreeMap::from([(1, leader.clone()), (2, follower.clone())]);
        check_page_repair(1, &reports).unwrap();
        let err = check_page_repair(2, &reports).unwrap_err();
        assert!(err.to_string().contains("group(s) 1"), "{err}");
        // Group 1's leader reports a completed cycle on another node.
        let mut other_leader = follower;
        other_leader.groups[1].page_repair_completed = true;
        let reports = BTreeMap::from([(1, leader), (2, other_leader)]);
        check_page_repair(2, &reports).unwrap();
    }

    /// Fake node: owns `led` groups, hosts `hosted` groups, all of which
    /// share one replicated level per group in `levels`.
    #[derive(Clone)]
    struct FakeNode {
        id: u64,
        supported: u32,
        led: Vec<u64>,
        levels: Arc<Mutex<BTreeMap<u64, u32>>>,
        posts: Arc<Mutex<u32>>,
    }

    async fn fake_metrics(State(fake): State<FakeNode>) -> Json<serde_json::Value> {
        let groups = [0u64, 1]
            .iter()
            .map(|group| {
                let leader = fake.led.contains(group).then_some(fake.id);
                serde_json::json!({
                    "raft_group_id": group,
                    "node_id": fake.id,
                    "current_leader": leader,
                    "voter_ids": [1, 2],
                    "learner_ids": [],
                })
            })
            .collect::<Vec<_>>();
        Json(serde_json::json!({ "raft_groups": groups }))
    }

    async fn fake_report(State(fake): State<FakeNode>) -> Json<serde_json::Value> {
        let levels = fake.levels.lock().unwrap().clone();
        let groups = levels
            .iter()
            .map(|(group, level)| {
                serde_json::json!({ "raft_group_id": group, "hosted": true, "level": level })
            })
            .collect::<Vec<_>>();
        Json(serde_json::json!({
            "version": 1,
            "node_id": fake.id,
            "supported_level": fake.supported,
            "groups": groups,
        }))
    }

    async fn fake_set(
        State(fake): State<FakeNode>,
        Json(body): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        *fake.posts.lock().unwrap() += 1;
        let level = u32::try_from(body["level"].as_u64().unwrap()).unwrap();
        let mut levels = fake.levels.lock().unwrap();
        let groups = levels
            .iter_mut()
            .map(|(group, current)| {
                if fake.led.contains(group) {
                    *current = (*current).max(level);
                    serde_json::json!({"raft_group_id": group, "status": "set", "level": *current})
                } else {
                    serde_json::json!({"raft_group_id": group, "status": "not_leader"})
                }
            })
            .collect::<Vec<_>>();
        Json(serde_json::json!({ "groups": groups }))
    }

    async fn spawn_fake(fake: FakeNode) -> NodeInfo {
        let app = Router::new()
            .route("/__ursula/metrics", get(fake_metrics))
            .route("/__ursula/feature-level", get(fake_report).post(fake_set))
            .with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        NodeInfo {
            id: fake.id,
            admin_url: Url::parse(&format!("http://{address}")).unwrap(),
            host: address.to_string(),
            http_url: None,
        }
    }

    fn options(level: u32) -> EnableFeatureOptions {
        EnableFeatureOptions {
            level,
            timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(10),
        }
    }

    #[tokio::test]
    async fn enable_feature_proposes_through_each_leader_and_verifies() {
        // One replicated level per group, shared by both replicas.
        let levels = Arc::new(Mutex::new(BTreeMap::from([(0, 0), (1, 0)])));
        let posts = Arc::new(Mutex::new(0));
        let one = spawn_fake(FakeNode {
            id: 1,
            supported: 1,
            led: vec![0],
            levels: levels.clone(),
            posts: posts.clone(),
        })
        .await;
        let two = spawn_fake(FakeNode {
            id: 2,
            supported: 1,
            led: vec![1],
            levels: levels.clone(),
            posts: posts.clone(),
        })
        .await;
        let client = MetricsClient::new(Duration::from_secs(2)).unwrap();
        let outcome = enable_feature(&client, &[one, two], &options(1))
            .await
            .unwrap();
        assert_eq!(outcome, EnableFeatureOutcome {
            level: 1,
            groups: 2,
            nodes: 2,
        });
        assert_eq!(*levels.lock().unwrap(), BTreeMap::from([(0, 1), (1, 1)]));
    }

    #[tokio::test]
    async fn enable_feature_refuses_before_proposing_when_a_node_is_old() {
        let levels = Arc::new(Mutex::new(BTreeMap::from([(0, 0), (1, 0)])));
        let posts = Arc::new(Mutex::new(0));
        let one = spawn_fake(FakeNode {
            id: 1,
            supported: 1,
            led: vec![0, 1],
            levels: levels.clone(),
            posts: posts.clone(),
        })
        .await;
        let two = spawn_fake(FakeNode {
            id: 2,
            supported: 0,
            led: vec![],
            levels: levels.clone(),
            posts: posts.clone(),
        })
        .await;
        let client = MetricsClient::new(Duration::from_secs(2)).unwrap();
        let err = enable_feature(&client, &[one, two], &options(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("node 2 supports feature level 0"));
        assert_eq!(*posts.lock().unwrap(), 0);
        assert_eq!(*levels.lock().unwrap(), BTreeMap::from([(0, 0), (1, 0)]));
    }
}
