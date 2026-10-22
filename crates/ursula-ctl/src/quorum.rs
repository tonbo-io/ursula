//! Fresh, fixed-prefix verification for recovery measurements and maintenance.
//!
//! Every group and its target voters come from the meta leader's view
//! ([`crate::view`]). This evidence is scoped to one observation.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use futures_util::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::QuorumPrefix;
use ursula_proto::placement::ProcessIncarnation;

use crate::MetricsClient;
use crate::NodeInfo;
use crate::metrics::ClusterSnapshot;
use crate::view::Targets;

type AppliedPrefixes = BTreeMap<u64, BTreeMap<u32, u64>>;

#[derive(Debug, Clone)]
pub struct QuorumVerificationOptions {
    pub timeout: Duration,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuorumVerification {
    pub version: ursula_proto::admin::SchemaVersion<4>,
    pub process_incarnations: BTreeMap<u64, ProcessIncarnation>,
    pub prefixes: BTreeMap<u32, QuorumPrefix>,
    pub applied: BTreeMap<u64, BTreeMap<u32, u64>>,
}

/// Evidence from the target voters other than one excluded node, not
/// restored redundancy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurvivingQuorumVerification {
    pub excluded_node_id: u64,
    pub surviving_node_ids: BTreeSet<u64>,
    pub full_redundancy_restored: bool,
    pub verification: QuorumVerification,
}

fn apply_evidence(
    snapshot: &ClusterSnapshot,
    targets: &Targets,
    prefixes: &BTreeMap<u32, QuorumPrefix>,
) -> Result<Option<AppliedPrefixes>> {
    let mut applied = BTreeMap::new();
    for node in &snapshot.per_node {
        let mut indices = BTreeMap::new();
        for (group_id, prefix) in prefixes {
            if !targets
                .get(group_id)
                .is_some_and(|target| target.contains(&node.node.id))
            {
                continue;
            }
            let group = node
                .group(u64::from(*group_id))
                .context("captured group is missing")?;
            if group.current_term != prefix.leader_term
                || group.current_leader != Some(prefix.leader_id)
            {
                bail!("group {group_id} leader changed after its fresh proof; sample again");
            }
            let index = group
                .last_applied_index
                .context("replica has not applied any prefix")?;
            if index < prefix.required_applied_index {
                return Ok(None);
            }
            indices.insert(*group_id, index);
        }
        applied.insert(node.node.id, indices);
    }
    Ok(Some(applied))
}

/// Capture one new outbound proof per group of the meta leader's view, then
/// wait for every target voter to apply those fixed prefixes under the same
/// leader terms. The absolute deadline includes network requests and sleeps;
/// writes can advance without moving the captured apply target indefinitely.
pub async fn verify_quorum(
    nodes: &[NodeInfo],
    client: &MetricsClient,
    options: &QuorumVerificationOptions,
) -> Result<QuorumVerification> {
    verify_observed_quorum(nodes, None, client, options).await
}

/// Confirm fresh prefixes on every target voter but `excluded_node_id`, a
/// node of the manifest. Each group keeps its whole target as its voters,
/// and the other target voters must form a quorum of it: excluding a node
/// from observation never narrows what a group must run on.
pub async fn verify_surviving_quorum(
    nodes: &[NodeInfo],
    excluded_node_id: u64,
    client: &MetricsClient,
    options: &QuorumVerificationOptions,
) -> Result<SurvivingQuorumVerification> {
    if !nodes.iter().any(|node| node.id == excluded_node_id) {
        bail!("the excluded node {excluded_node_id} is not in the manifest");
    }
    let verification =
        verify_observed_quorum(nodes, Some(excluded_node_id), client, options).await?;
    Ok(SurvivingQuorumVerification {
        excluded_node_id,
        surviving_node_ids: nodes
            .iter()
            .map(|node| node.id)
            .filter(|id| *id != excluded_node_id)
            .collect(),
        full_redundancy_restored: false,
        verification,
    })
}

async fn verify_observed_quorum(
    nodes: &[NodeInfo],
    excluded: Option<u64>,
    client: &MetricsClient,
    options: &QuorumVerificationOptions,
) -> Result<QuorumVerification> {
    let ids = nodes.iter().map(|node| node.id).collect::<BTreeSet<_>>();
    if nodes.is_empty() || ids.len() != nodes.len() {
        bail!("quorum verification requires a manifest of unique nodes");
    }
    let observed = nodes
        .iter()
        .filter(|node| Some(node.id) != excluded)
        .cloned()
        .collect::<Vec<_>>();
    let observe = async {
        let targets = crate::view::read_view(client, nodes).await??.targets()?;
        let initial = observe_on_targets(client, &observed, &targets, excluded).await?;
        let mut probes = Vec::new();
        for (&group_id, target) in &targets {
            let leaders = target
                .iter()
                .filter_map(|id| initial.node(*id))
                .filter_map(|node| {
                    node.group(u64::from(group_id))
                        .and_then(|group| group.current_leader)
                })
                .collect::<BTreeSet<_>>();
            let mut leaders = leaders.into_iter();
            let (Some(leader_id), None) = (leaders.next(), leaders.next()) else {
                bail!("group {group_id} has inconsistent observed leaders");
            };
            let leader = observed
                .iter()
                .find(|node| node.id == leader_id)
                .with_context(|| {
                    format!("group {group_id} is led by node {leader_id}, which is not observed")
                })?;
            probes.push(async move {
                let proof = client.confirm_quorum(leader, group_id).await?;
                Ok::<_, anyhow::Error>((group_id, proof))
            });
        }
        let prefixes = futures_util::stream::iter(probes)
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<BTreeMap<_, _>>>()?;
        loop {
            let snapshot = observe_on_targets(client, &observed, &targets, excluded).await?;
            match apply_evidence(&snapshot, &targets, &prefixes)? {
                Some(applied) => {
                    let process_incarnations = snapshot
                        .per_node
                        .iter()
                        .map(|node| (node.node.id, node.process_incarnation))
                        .collect::<BTreeMap<_, _>>();
                    return Ok(QuorumVerification {
                        version: ursula_proto::admin::SchemaVersion,
                        process_incarnations,
                        prefixes,
                        applied,
                    });
                }
                None => tracing::debug!("waiting for captured quorum prefixes"),
            }
            tokio::time::sleep(options.poll_interval).await;
        }
    };
    tokio::time::timeout(options.timeout, observe)
        .await
        .context("fresh quorum verification deadline reached")?
}

/// One observation of `nodes` whose maintenance reports are ready and on
/// which every group runs on its target voters other than `excluded`.
async fn observe_on_targets(
    client: &MetricsClient,
    nodes: &[NodeInfo],
    targets: &Targets,
    excluded: Option<u64>,
) -> Result<ClusterSnapshot> {
    let snapshot = client.try_fetch_cluster(nodes).await;
    crate::observe::check_maintenance_snapshot(&snapshot)?;
    crate::view::check_groups(targets, &snapshot, excluded)?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    #[test]
    fn proof_has_one_schema_and_no_vacuous_certification_fields() {
        let proof = super::QuorumVerification {
            version: ursula_proto::admin::SchemaVersion,
            process_incarnations: Default::default(),
            prefixes: Default::default(),
            applied: Default::default(),
        };
        let value = serde_json::to_value(proof).unwrap();
        assert_eq!(value["version"], 4);
        for field in [
            "participation_certified",
            "process_incarnations_certified",
            "maintenance_executor_certified",
            "maintenance_executor_retired_certified",
            "maintenance_fence",
        ] {
            assert!(value.get(field).is_none());
            let mut legacy = value.clone();
            legacy[field] = serde_json::Value::Bool(true);
            let error = serde_json::from_value::<super::QuorumVerification>(legacy).unwrap_err();
            assert_eq!(error.classify(), serde_json::error::Category::Data);
        }
        for version in [0, 1, 2, 3, 5, u32::MAX] {
            let mut unknown = value.clone();
            unknown["version"] = serde_json::Value::from(version);
            let error = serde_json::from_value::<super::QuorumVerification>(unknown).unwrap_err();
            assert_eq!(error.classify(), serde_json::error::Category::Data);
        }
        let decoded: super::QuorumVerification = serde_json::from_value(value).unwrap();
        assert_eq!(u32::from(decoded.version), 4);
    }

    use tokio::time::Instant;

    use super::*;

    fn snapshot() -> ClusterSnapshot {
        ClusterSnapshot {
            per_node: (1..=3)
                .map(|id| {
                    let node = NodeInfo {
                        id,
                        admin_url: format!("http://node-{id}:4438").parse().unwrap(),
                        http_url: Some(format!("http://node-{id}:4437").parse().unwrap()),
                        metrics_url: None,
                        client_url: None,
                        host: format!("node-{id}"),
                    };
                    let groups = (0..2)
                        .map(|raft_group_id| {
                            crate::metrics::test_group(
                                raft_group_id,
                                id,
                                7,
                                Some(1),
                                Some(25),
                                Some(20),
                                vec![1, 2, 3],
                            )
                        })
                        .collect();
                    crate::metrics::test_view(node, groups)
                })
                .collect(),
        }
    }

    fn targets() -> Targets {
        (0..2)
            .map(|group| (group, BTreeSet::from([1, 2, 3])))
            .collect()
    }

    fn prefixes(index: u64) -> BTreeMap<u32, QuorumPrefix> {
        (0..2)
            .map(|raft_group_id| {
                (raft_group_id, QuorumPrefix {
                    raft_group_id,
                    leader_id: 1,
                    leader_term: 7,
                    required_applied_index: index,
                })
            })
            .collect()
    }

    #[test]
    fn captured_apply_target_does_not_follow_continuous_commits() {
        let sample = snapshot();
        assert!(
            apply_evidence(&sample, &targets(), &prefixes(20))
                .unwrap()
                .is_some()
        );
        assert!(
            apply_evidence(&sample, &targets(), &prefixes(21))
                .unwrap()
                .is_none()
        );
    }

    /// A node outside a group's target is not asked for that group's
    /// prefix: node 3 runs no replica of group 1.
    #[test]
    fn only_a_groups_target_voters_apply_its_prefix() {
        let mut sample = snapshot();
        sample.per_node[2].groups.pop();
        let mut targets = targets();
        targets.insert(1, BTreeSet::from([1, 2, 4]));
        let applied = apply_evidence(&sample, &targets, &prefixes(20))
            .unwrap()
            .unwrap();
        assert_eq!(applied[&3], BTreeMap::from([(0, 20)]));
        assert_eq!(applied[&1], BTreeMap::from([(0, 20), (1, 20)]));
    }

    #[tokio::test]
    async fn survivor_api_refuses_an_exclusion_outside_the_manifest() {
        let nodes = snapshot()
            .per_node
            .into_iter()
            .map(|node| node.node)
            .collect::<Vec<_>>();
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let options = QuorumVerificationOptions {
            timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(10),
        };
        let error = verify_surviving_quorum(&nodes, 4, &client, &options)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not in the manifest"), "{error}");
    }

    #[test]
    fn stale_term_or_changed_leader_cannot_reuse_a_prefix() {
        let mut sample = snapshot();
        sample.per_node[2].groups[0].current_term = 8;
        apply_evidence(&sample, &targets(), &prefixes(20))
            .expect_err("a stale term must not reuse a prefix");
        sample.per_node[2].groups[0].current_term = 7;
        sample.per_node[2].groups[0].current_leader = Some(2);
        apply_evidence(&sample, &targets(), &prefixes(20))
            .expect_err("a changed leader must not reuse a prefix");
    }

    #[test]
    fn missing_maintenance_reports_never_certify_participation() {
        let mut sample = snapshot();
        sample.per_node[0].raft_maintenance = None;
        assert!(matches!(
            crate::observe::check_maintenance_snapshot(&sample),
            Err(crate::observe::ReadinessRefusal::Maintenance { node_id: 1, .. })
        ));
    }

    #[tokio::test]
    async fn absolute_deadline_includes_a_blocked_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().fallback(|| async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            axum::Json(serde_json::json!({}))
        });
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut nodes = snapshot()
            .per_node
            .into_iter()
            .map(|node| node.node)
            .collect::<Vec<_>>();
        for node in &mut nodes {
            node.metrics_url = Some(format!("http://{address}").parse().unwrap());
        }
        let started = Instant::now();
        let result = verify_quorum(
            &nodes,
            &MetricsClient::new(Duration::from_secs(30)).unwrap(),
            &QuorumVerificationOptions {
                timeout: Duration::from_millis(25),
                poll_interval: Duration::from_secs(1),
            },
        )
        .await;
        server.abort();
        assert!(result.unwrap_err().to_string().contains("deadline"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
