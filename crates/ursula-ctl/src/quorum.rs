//! Fresh, fixed-prefix verification for recovery measurements and maintenance.
//!
//! This evidence is scoped to one observation. Physical disruption still needs
//! an exclusive reservation and incarnation-aware lifecycle fencing.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use futures_util::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::MaintenanceFence;
use ursula_proto::admin::MaintenanceFenceState;
use ursula_proto::admin::ProcessIncarnation;
use ursula_proto::admin::QuorumPrefix;

use crate::MetricsClient;
use crate::NodeInfo;
use crate::metrics::ClusterSnapshot;

type AppliedPrefixes = BTreeMap<u64, BTreeMap<u32, u64>>;

#[derive(Debug, Clone)]
pub struct QuorumVerificationOptions {
    pub group_count: u32,
    pub timeout: Duration,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuorumVerification {
    pub version: ursula_proto::admin::SchemaVersion<3>,
    pub process_incarnations: BTreeMap<u64, ProcessIncarnation>,
    /// Process-local admission only, not proof of an external CAS reservation.
    pub maintenance_executor_certified: bool,
    pub maintenance_executor_retired_certified: bool,
    pub maintenance_fence: Option<MaintenanceFence>,
    pub prefixes: BTreeMap<u32, QuorumPrefix>,
    pub applied: BTreeMap<u64, BTreeMap<u32, u64>>,
}

fn certified_executor(snapshot: &ClusterSnapshot, retired: bool) -> Option<MaintenanceFence> {
    let expected = snapshot
        .per_node
        .first()?
        .node
        .expected_maintenance_fence
        .as_ref()?;
    let expected_state = if retired {
        MaintenanceFenceState::Retired {
            fence: expected.clone(),
        }
    } else {
        MaintenanceFenceState::Active {
            fence: expected.clone(),
        }
    };
    snapshot
        .per_node
        .iter()
        .all(|node| {
            node.node.expected_maintenance_fence.as_ref() == Some(expected)
                && node.maintenance_fence == expected_state
                && !node.maintenance_fence_uncertain
        })
        .then(|| expected.clone())
}

/// Evidence from the two observed survivors, not restored three-voter
/// redundancy or evidence that the excluded physical host is fenced.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurvivingQuorumVerification {
    pub excluded_voter_id: u64,
    pub configured_voter_ids: BTreeSet<u64>,
    pub surviving_voter_ids: BTreeSet<u64>,
    pub full_redundancy_restored: bool,
    pub verification: QuorumVerification,
}

#[cfg(test)]
fn validate_inventory(
    snapshot: &ClusterSnapshot,
    voters: &BTreeSet<u64>,
    group_count: u32,
) -> Result<()> {
    validate_observed_inventory(snapshot, voters, voters, group_count)
}

fn validate_observed_inventory(
    snapshot: &ClusterSnapshot,
    voters: &BTreeSet<u64>,
    observed_voters: &BTreeSet<u64>,
    group_count: u32,
) -> Result<()> {
    if !observed_voters.is_subset(voters) || observed_voters.len() <= voters.len() / 2 {
        bail!("observed configured voters cannot form a quorum");
    }
    let reported = snapshot
        .per_node
        .iter()
        .map(|node| node.node.id)
        .collect::<BTreeSet<_>>();
    if &reported != observed_voters || snapshot.per_node.len() != observed_voters.len() {
        bail!("quorum verification requires every selected voter exactly once");
    }
    let expected = (0..u64::from(group_count)).collect::<BTreeSet<_>>();
    for node in &snapshot.per_node {
        let inventory = node
            .groups
            .iter()
            .map(|group| group.raft_group_id)
            .collect::<BTreeSet<_>>();
        if inventory != expected || node.groups.len() != expected.len() {
            bail!(
                "node {} group inventory differs from configuration",
                node.node.id
            );
        }
        if let Some(report) = &node.raft_maintenance {
            if !report.ready()
                || report.node_id != node.node.id
                || report
                    .expected_groups
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    != (0..group_count).collect()
                || report
                    .expected_groups
                    .values()
                    .any(|expected| expected != voters)
            {
                bail!(
                    "node {} maintenance report is ineligible or misconfigured",
                    node.node.id
                );
            }
        } else {
            bail!("metrics cannot certify participation without a maintenance report");
        }
        for group in &node.groups {
            if group.node_id != node.node.id
                || group.voter_ids.iter().copied().collect::<BTreeSet<_>>() != *voters
                || group.voter_ids.len() != voters.len()
                || !group.learner_ids.is_empty()
                || !group.participation_ready()
                || group.last_applied_index.is_none()
                || group.committed_index.is_none()
                || !group
                    .current_leader
                    .is_some_and(|leader| voters.contains(&leader))
            {
                bail!(
                    "node {} group {} is not fully eligible",
                    node.node.id,
                    group.raft_group_id
                );
            }
        }
    }
    Ok(())
}

fn apply_evidence(
    snapshot: &ClusterSnapshot,
    prefixes: &BTreeMap<u32, QuorumPrefix>,
) -> Result<Option<AppliedPrefixes>> {
    let mut applied = BTreeMap::new();
    for node in &snapshot.per_node {
        let mut indices = BTreeMap::new();
        for (group_id, prefix) in prefixes {
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

/// Capture one new outbound proof per configured group, then wait for every
/// replica to apply those fixed prefixes under the same leader terms. The
/// absolute deadline includes network requests and sleeps; writes can advance
/// without moving the captured apply target indefinitely.
pub async fn verify_quorum(
    nodes: &[NodeInfo],
    client: &MetricsClient,
    options: &QuorumVerificationOptions,
) -> Result<QuorumVerification> {
    verify_observed_quorum(nodes, nodes, client, options).await
}

/// Confirm fresh prefixes on both survivors after excluding exactly one of
/// three configured voters. Full membership still names the original three;
/// narrowing the observed set must never narrow the expected membership.
pub async fn verify_surviving_quorum(
    configured_nodes: &[NodeInfo],
    excluded_voter_id: u64,
    client: &MetricsClient,
    options: &QuorumVerificationOptions,
) -> Result<SurvivingQuorumVerification> {
    let configured_voter_ids = configured_nodes
        .iter()
        .map(|node| node.id)
        .collect::<BTreeSet<_>>();
    if configured_nodes.len() != 3
        || configured_voter_ids.len() != 3
        || !configured_voter_ids.contains(&excluded_voter_id)
    {
        bail!("survivor observation requires three configured voters and one known exclusion");
    }
    let survivors = configured_nodes
        .iter()
        .filter(|node| node.id != excluded_voter_id)
        .cloned()
        .collect::<Vec<_>>();
    let verification =
        verify_observed_quorum(configured_nodes, &survivors, client, options).await?;
    Ok(SurvivingQuorumVerification {
        excluded_voter_id,
        configured_voter_ids,
        surviving_voter_ids: survivors.iter().map(|node| node.id).collect(),
        full_redundancy_restored: false,
        verification,
    })
}

async fn verify_observed_quorum(
    configured_nodes: &[NodeInfo],
    nodes: &[NodeInfo],
    client: &MetricsClient,
    options: &QuorumVerificationOptions,
) -> Result<QuorumVerification> {
    let voters = configured_nodes
        .iter()
        .map(|node| node.id)
        .collect::<BTreeSet<_>>();
    let observed_voters = nodes.iter().map(|node| node.id).collect::<BTreeSet<_>>();
    if nodes.is_empty()
        || voters.len() != configured_nodes.len()
        || observed_voters.len() != nodes.len()
        || options.group_count == 0
    {
        bail!("quorum verification requires unique voters and nonempty configured groups");
    }
    let observe = async {
        let initial = client.fetch_cluster(nodes).await?;
        validate_observed_inventory(&initial, &voters, &observed_voters, options.group_count)?;
        let mut probes = Vec::new();
        for group_id in 0..options.group_count {
            let leaders = initial
                .per_node
                .iter()
                .filter_map(|node| {
                    node.group(u64::from(group_id))
                        .and_then(|group| group.current_leader)
                })
                .collect::<BTreeSet<_>>();
            if leaders.len() != 1 {
                bail!("group {group_id} has inconsistent observed leaders");
            }
            let leader_id = leaders
                .iter()
                .next()
                .copied()
                .context("leader is missing")?;
            let leader = nodes
                .iter()
                .find(|node| node.id == leader_id)
                .context("leader is outside configuration")?;
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
            let snapshot = client.fetch_cluster(nodes).await?;
            validate_observed_inventory(&snapshot, &voters, &observed_voters, options.group_count)?;
            match apply_evidence(&snapshot, &prefixes)? {
                Some(applied) => {
                    let process_incarnations = snapshot
                        .per_node
                        .iter()
                        .map(|node| (node.node.id, node.process_incarnation.clone()))
                        .collect::<BTreeMap<_, _>>();
                    let active_fence = certified_executor(&snapshot, false);
                    let retired_fence = certified_executor(&snapshot, true);
                    return Ok(QuorumVerification {
                        version: ursula_proto::admin::SchemaVersion,
                        maintenance_executor_certified: active_fence.is_some(),
                        maintenance_executor_retired_certified: retired_fence.is_some(),
                        maintenance_fence: active_fence.or(retired_fence),
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

#[cfg(test)]
mod tests {
    #[test]
    fn proof_has_one_schema_and_no_vacuous_certification_fields() {
        let proof = super::QuorumVerification {
            version: ursula_proto::admin::SchemaVersion,
            process_incarnations: Default::default(),
            maintenance_executor_certified: false,
            maintenance_executor_retired_certified: false,
            maintenance_fence: None,
            prefixes: Default::default(),
            applied: Default::default(),
        };
        let value = serde_json::to_value(proof).unwrap();
        assert_eq!(value["version"], 3);
        for field in ["participation_certified", "process_incarnations_certified"] {
            assert!(value.get(field).is_none());
            let mut legacy = value.clone();
            legacy[field] = serde_json::Value::Bool(true);
            let error = serde_json::from_value::<super::QuorumVerification>(legacy).unwrap_err();
            assert_eq!(error.classify(), serde_json::error::Category::Data);
        }
        for version in [0, 1, 2, 4, u32::MAX] {
            let mut unknown = value.clone();
            unknown["version"] = serde_json::Value::from(version);
            let error = serde_json::from_value::<super::QuorumVerification>(unknown).unwrap_err();
            assert_eq!(error.classify(), serde_json::error::Category::Data);
        }
        let decoded: super::QuorumVerification = serde_json::from_value(value).unwrap();
        assert_eq!(u32::from(decoded.version), 3);
    }

    use tokio::time::Instant;

    use super::*;

    fn snapshot() -> ClusterSnapshot {
        ClusterSnapshot {
            per_node: (1..=3)
                .map(|id| {
                    let node = NodeInfo {
                        expected_process_incarnation: None,
                        expected_maintenance_fence: None,
                        id,
                        admin_url: format!("http://node-{id}:4438").parse().unwrap(),
                        http_url: Some(format!("http://node-{id}:4437").parse().unwrap()),
                        metrics_url: None,
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

    #[test]
    fn executor_evidence_requires_same_saved_token_and_healthy_admission_everywhere() {
        let fence = MaintenanceFence::new(format!("{:032x}", 1), format!("{:032x}", 2), 3).unwrap();
        let mut state = snapshot();
        assert_eq!(certified_executor(&state, false), None);
        for node in &mut state.per_node {
            node.process_incarnation = ProcessIncarnation::from_bits(u128::from(node.node.id));
            node.node.expected_maintenance_fence = Some(fence.clone());
            node.maintenance_fence = MaintenanceFenceState::Active {
                fence: fence.clone(),
            };
        }
        assert_eq!(certified_executor(&state, false), Some(fence.clone()));
        state.per_node[1].maintenance_fence_uncertain = true;
        assert_eq!(certified_executor(&state, false), None);
        state.per_node[1].maintenance_fence_uncertain = false;
        state.per_node[1].maintenance_fence = MaintenanceFenceState::Retired { fence };
        assert_eq!(certified_executor(&state, false), None);
        assert_eq!(certified_executor(&state, true), None);
        let token = state.per_node[0]
            .node
            .expected_maintenance_fence
            .clone()
            .unwrap();
        for node in &mut state.per_node {
            node.maintenance_fence = MaintenanceFenceState::Retired {
                fence: token.clone(),
            };
        }
        assert_eq!(certified_executor(&state, false), None);
        assert_eq!(certified_executor(&state, true), Some(token));
        state.per_node[1].maintenance_fence = state.per_node[0].maintenance_fence.clone();
        state.per_node[1].node.expected_maintenance_fence = None;
        assert_eq!(certified_executor(&state, false), None);
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
        validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2).unwrap();
        assert!(apply_evidence(&sample, &prefixes(20)).unwrap().is_some());
        assert!(apply_evidence(&sample, &prefixes(21)).unwrap().is_none());
    }

    #[test]
    fn survivor_observation_preserves_the_full_configured_membership() {
        let mut sample = snapshot();
        sample.per_node.retain(|node| node.node.id != 3);
        let voters = BTreeSet::from([1, 2, 3]);
        let survivors = BTreeSet::from([1, 2]);
        validate_observed_inventory(&sample, &voters, &survivors, 2).unwrap();
        validate_inventory(&sample, &voters, 2)
            .expect_err("inventory without the third voter must be rejected");
        assert_eq!(
            apply_evidence(&sample, &prefixes(20))
                .unwrap()
                .unwrap()
                .len(),
            2
        );
        for node in &mut sample.per_node {
            for group in &mut node.groups {
                group.voter_ids = vec![1, 2];
            }
        }
        validate_observed_inventory(&sample, &voters, &survivors, 2)
            .expect_err("survivors that drop a configured voter must be rejected");
    }

    #[test]
    fn survivor_observation_cannot_exclude_a_second_required_replica() {
        let mut sample = snapshot();
        sample.per_node.retain(|node| node.node.id == 1);
        validate_observed_inventory(&sample, &BTreeSet::from([1, 2, 3]), &BTreeSet::from([1]), 2)
            .expect_err("survivor observation of a single replica must be rejected");
        validate_observed_inventory(
            &sample,
            &BTreeSet::from([1, 2, 3]),
            &BTreeSet::from([1, 2]),
            2,
        )
        .expect_err(
            "survivor observation that excludes a second required replica must be rejected",
        );
        validate_observed_inventory(
            &sample,
            &BTreeSet::from([1, 2, 3]),
            &BTreeSet::from([1, 4]),
            2,
        )
        .expect_err("survivor observation that names an unknown node must be rejected");
    }

    #[tokio::test]
    async fn survivor_api_refuses_unknown_exclusions_or_a_changed_topology() {
        let nodes = snapshot()
            .per_node
            .into_iter()
            .map(|node| node.node)
            .collect::<Vec<_>>();
        let client = MetricsClient::new(Duration::from_secs(1)).unwrap();
        let options = QuorumVerificationOptions {
            group_count: 2,
            timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(10),
        };
        for (manifest, excluded) in [(&nodes[..], 4), (&nodes[..2], 1)] {
            let error = verify_surviving_quorum(manifest, excluded, &client, &options)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("three configured voters"),
                "{error}"
            );
        }
    }

    #[test]
    fn missing_group_on_every_node_does_not_shrink_expected_inventory() {
        let mut sample = snapshot();
        for node in &mut sample.per_node {
            node.groups.pop();
        }
        validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2)
            .expect_err("a group missing on every node must be rejected");
    }

    #[test]
    fn duplicate_node_or_group_is_rejected() {
        let mut sample = snapshot();
        sample.per_node[2] = sample.per_node[1].clone();
        validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2)
            .expect_err("a duplicate node must be rejected");
        let mut sample = snapshot();
        sample.per_node[0].groups[1] = sample.per_node[0].groups[0].clone();
        validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2)
            .expect_err("a duplicate group must be rejected");
    }

    #[test]
    fn stale_term_or_changed_leader_cannot_reuse_a_prefix() {
        let mut sample = snapshot();
        sample.per_node[2].groups[0].current_term = 8;
        apply_evidence(&sample, &prefixes(20)).expect_err("a stale term must not reuse a prefix");
        sample.per_node[2].groups[0].current_term = 7;
        sample.per_node[2].groups[0].current_leader = Some(2);
        apply_evidence(&sample, &prefixes(20))
            .expect_err("a changed leader must not reuse a prefix");
    }

    #[test]
    fn missing_maintenance_reports_never_certify_participation() {
        let mut sample = snapshot();
        sample.per_node[0].raft_maintenance = None;
        validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2).expect_err(
            "legacy metrics must not certify participation without a maintenance report",
        );
    }

    #[test]
    fn learner_joint_membership_or_closed_recovery_is_ineligible() {
        for mode in 0..3 {
            let mut sample = snapshot();
            let group = &mut sample.per_node[0].groups[0];
            match mode {
                0 => group.learner_ids.push(4),
                1 => group.maintenance.membership_joint = true,
                _ => group.maintenance.recovery_ready = false,
            }
            validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2)
                .expect_err("a learner, joint membership or closed recovery must be ineligible");
        }
    }

    #[tokio::test]
    async fn absolute_deadline_includes_a_blocked_metrics_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route(
            "/__ursula/metrics",
            axum::routing::get(|| async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                axum::Json(serde_json::json!({}))
            }),
        );
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
                group_count: 2,
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
