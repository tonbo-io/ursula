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
use serde::Serialize;
use tokio::time::Instant;
use ursula_raft::QuorumPrefix;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use crate::MetricsClient;
use crate::NodeInfo;
use crate::metrics::ClusterSnapshot;

type AppliedPrefixes = BTreeMap<u64, BTreeMap<u32, u64>>;

#[derive(Debug, Clone)]
pub struct QuorumVerificationOptions {
    pub group_count: u32,
    pub core_count: u16,
    pub timeout: Duration,
    pub poll_interval: Duration,
    /// Diagnostic compatibility for the pinned 0.6.2 baseline only. Such a
    /// result explicitly cannot certify process-local participation gates.
    pub allow_legacy_eligibility: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuorumVerification {
    pub version: u32,
    pub participation_certified: bool,
    pub prefixes: BTreeMap<u32, QuorumPrefix>,
    pub applied: BTreeMap<u64, BTreeMap<u32, u64>>,
}

/// Evidence from the two observed survivors, not restored three-voter
/// redundancy or evidence that the excluded physical host is fenced.
#[derive(Debug, Clone, Serialize)]
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
    allow_legacy: bool,
) -> Result<bool> {
    validate_observed_inventory(snapshot, voters, voters, group_count, allow_legacy)
}

fn validate_observed_inventory(
    snapshot: &ClusterSnapshot,
    voters: &BTreeSet<u64>,
    observed_voters: &BTreeSet<u64>,
    group_count: u32,
    allow_legacy: bool,
) -> Result<bool> {
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
    let mut participation_certified = true;
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
            participation_certified = false;
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
                || group.current_term.is_none()
            {
                bail!(
                    "node {} group {} is not fully eligible",
                    node.node.id,
                    group.raft_group_id
                );
            }
            if group.maintenance.is_none() {
                participation_certified = false;
            }
        }
    }
    if !participation_certified && !allow_legacy {
        bail!("legacy metrics cannot certify participation; only baseline diagnostics may opt in");
    }
    Ok(participation_certified)
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
            if group.current_term != Some(prefix.leader_term)
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
        || options.core_count == 0
    {
        bail!("quorum verification requires unique voters and nonempty configured groups/cores");
    }
    let deadline = Instant::now() + options.timeout;
    let observe = async {
        let initial = client.fetch_cluster(nodes).await?;
        validate_observed_inventory(
            &initial,
            &voters,
            &observed_voters,
            options.group_count,
            options.allow_legacy_eligibility,
        )?;
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
            let endpoint = leader
                .http_url
                .as_ref()
                .context("quorum proof needs the actual Raft/client endpoint")?;
            if endpoint.path() != "/" || endpoint.query().is_some() || endpoint.fragment().is_some()
            {
                bail!("Raft/client endpoint must not contain a path, query or fragment");
            }
            let placement = ShardPlacement {
                raft_group_id: RaftGroupId(group_id),
                shard_id: ShardId(group_id),
                core_id: CoreId(u16::try_from(
                    group_id
                        .checked_rem(u32::from(options.core_count))
                        .context("core count is zero")?,
                )?),
            };
            probes.push(async move {
                let proof = ursula_raft::confirm_quorum_prefix(
                    placement,
                    leader_id,
                    endpoint.as_str(),
                    client.timeout(),
                )
                .await
                .map_err(anyhow::Error::msg)?;
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
            let participation_certified = validate_observed_inventory(
                &snapshot,
                &voters,
                &observed_voters,
                options.group_count,
                options.allow_legacy_eligibility,
            )?;
            match apply_evidence(&snapshot, &prefixes)? {
                Some(applied) => {
                    return Ok(QuorumVerification {
                        version: 1,
                        participation_certified,
                        prefixes,
                        applied,
                    });
                }
                None => tracing::debug!("waiting for captured quorum prefixes"),
            }
            tokio::time::sleep(options.poll_interval).await;
        }
    };
    tokio::time::timeout_at(deadline, observe)
        .await
        .context("fresh quorum verification deadline reached")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::NodeMetricsView;
    use crate::metrics::RaftGroupView;

    fn snapshot() -> ClusterSnapshot {
        ClusterSnapshot {
            per_node: (1..=3)
                .map(|id| NodeMetricsView {
                    node: NodeInfo {
                        id,
                        admin_url: format!("http://node-{id}:4438").parse().unwrap(),
                        http_url: Some(format!("http://node-{id}:4437").parse().unwrap()),
                        metrics_url: None,
                        host: format!("node-{id}"),
                    },
                    groups: (0..2)
                        .map(|raft_group_id| RaftGroupView {
                            raft_group_id,
                            node_id: id,
                            current_term: Some(7),
                            current_leader: Some(1),
                            committed_index: Some(25),
                            last_applied_index: Some(20),
                            voter_ids: vec![1, 2, 3],
                            learner_ids: vec![],
                            maintenance: Some(ursula_raft::RaftGroupMaintenanceState {
                                running: true,
                                recovery_ready: true,
                                membership_joint: false,
                                membership_log_index: Some(2),
                                stopped_for_operator: false,
                                accepting_transfers: true,
                            }),
                        })
                        .collect(),
                    wal_backend: Some("memory".to_owned()),
                    raft_maintenance: Some(ursula_raft::RaftMaintenanceReport {
                        version: 1,
                        node_id: id,
                        lag_tolerance: 16,
                        expected_groups: (0..2)
                            .map(|group| (group, BTreeSet::from([1, 2, 3])))
                            .collect(),
                        node_issues: vec![],
                        group_issues: BTreeMap::new(),
                    }),
                })
                .collect(),
        }
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
        assert!(validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2, false).unwrap());
        assert!(apply_evidence(&sample, &prefixes(20)).unwrap().is_some());
        assert!(apply_evidence(&sample, &prefixes(21)).unwrap().is_none());
    }

    #[test]
    fn survivor_observation_preserves_the_full_configured_membership() {
        let mut sample = snapshot();
        sample.per_node.retain(|node| node.node.id != 3);
        let voters = BTreeSet::from([1, 2, 3]);
        let survivors = BTreeSet::from([1, 2]);
        assert!(validate_observed_inventory(&sample, &voters, &survivors, 2, false).unwrap());
        assert!(validate_inventory(&sample, &voters, 2, false).is_err());
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
        assert!(validate_observed_inventory(&sample, &voters, &survivors, 2, false).is_err());
    }

    #[test]
    fn survivor_observation_cannot_exclude_a_second_required_replica() {
        let mut sample = snapshot();
        sample.per_node.retain(|node| node.node.id == 1);
        assert!(
            validate_observed_inventory(
                &sample,
                &BTreeSet::from([1, 2, 3]),
                &BTreeSet::from([1]),
                2,
                false
            )
            .is_err()
        );
        assert!(
            validate_observed_inventory(
                &sample,
                &BTreeSet::from([1, 2, 3]),
                &BTreeSet::from([1, 2]),
                2,
                false
            )
            .is_err()
        );
        assert!(
            validate_observed_inventory(
                &sample,
                &BTreeSet::from([1, 2, 3]),
                &BTreeSet::from([1, 4]),
                2,
                false
            )
            .is_err()
        );
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
            core_count: 1,
            timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(10),
            allow_legacy_eligibility: false,
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
        assert!(validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2, false).is_err());
    }

    #[test]
    fn duplicate_node_or_group_is_rejected() {
        let mut sample = snapshot();
        sample.per_node[2] = sample.per_node[1].clone();
        assert!(validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2, false).is_err());
        let mut sample = snapshot();
        sample.per_node[0].groups[1] = sample.per_node[0].groups[0].clone();
        assert!(validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2, false).is_err());
    }

    #[test]
    fn stale_term_or_changed_leader_cannot_reuse_a_prefix() {
        let mut sample = snapshot();
        sample.per_node[2].groups[0].current_term = Some(8);
        assert!(apply_evidence(&sample, &prefixes(20)).is_err());
        sample.per_node[2].groups[0].current_term = Some(7);
        sample.per_node[2].groups[0].current_leader = Some(2);
        assert!(apply_evidence(&sample, &prefixes(20)).is_err());
    }

    #[test]
    fn legacy_opt_in_never_certifies_participation() {
        let mut sample = snapshot();
        sample.per_node[0].raft_maintenance = None;
        sample.per_node[0].groups[0].maintenance = None;
        assert!(validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2, false).is_err());
        assert!(!validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2, true).unwrap());
    }

    #[test]
    fn learner_joint_membership_or_closed_recovery_is_ineligible() {
        for mode in 0..3 {
            let mut sample = snapshot();
            let group = &mut sample.per_node[0].groups[0];
            match mode {
                0 => group.learner_ids.push(4),
                1 => group.maintenance.as_mut().unwrap().membership_joint = true,
                _ => group.maintenance.as_mut().unwrap().recovery_ready = false,
            }
            assert!(validate_inventory(&sample, &BTreeSet::from([1, 2, 3]), 2, true).is_err());
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
                core_count: 2,
                timeout: Duration::from_millis(25),
                poll_interval: Duration::from_secs(1),
                allow_legacy_eligibility: false,
            },
        )
        .await;
        server.abort();
        assert!(result.unwrap_err().to_string().contains("deadline"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
