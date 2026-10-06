//! Read-only, per-group quorum evidence for managed subset and mixed-RF layouts.
//! Native Raft confirms each uniform/joint configuration; metrics only attest
//! pinned processes' fixed-prefix application. This is never a disruption lease.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use futures_util::StreamExt;
use serde::Serialize;
use ursula_control::CommittedGroupConfiguration;
use ursula_control::ControlProjection;
use ursula_control::NodeState;
use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use crate::MetricsClient;
use crate::NodeInfo;
use crate::metrics::ClusterSnapshot;
use crate::operations::OperationClient;

#[derive(Debug, Clone)]
pub struct ManagedQuorumOptions {
    pub timeout: Duration,
    pub poll_interval: Duration,
    /// Observations exclude these processes without changing expected voters.
    pub excluded_nodes: BTreeSet<u64>,
}

#[derive(Debug, Serialize)]
pub struct ManagedGroupQuorum {
    pub configuration: CommittedGroupConfiguration,
    pub observed_voters: BTreeSet<u64>,
    pub required_majorities: Vec<usize>,
    pub observed_per_set: Vec<usize>,
    pub applied: BTreeMap<u64, u64>,
    pub full_redundancy_observed: bool,
}

#[derive(Debug, Serialize)]
pub struct ManagedQuorumVerification {
    pub version: u32,
    pub applied_meta_index: u64,
    pub active_migration_id: Option<u64>,
    pub excluded_nodes: BTreeSet<u64>,
    pub process_incarnations: BTreeMap<u64, ProcessIncarnation>,
    pub groups: BTreeMap<u32, ManagedGroupQuorum>,
    pub maintenance_eligible: bool,
    pub disruption_authorized: bool,
}

fn validate_configuration(
    view: &ControlProjection,
    observed: &CommittedGroupConfiguration,
) -> Result<()> {
    observed.validate().map_err(anyhow::Error::msg)?;
    let placement = view
        .state
        .placements
        .get(&observed.raft_group_id)
        .context("unassigned data group")?;
    if let Some(migration) = view
        .state
        .active_migration()
        .filter(|migration| migration.raft_group_id == observed.raft_group_id)
    {
        let managed = migration
            .managed
            .as_ref()
            .context("migration lacks managed authority")?;
        let source = vec![migration.from_voters.clone()];
        let target = vec![migration.target_voters.clone()];
        let joint = vec![
            migration.from_voters.clone(),
            migration.target_voters.clone(),
        ];
        if observed.voter_sets != source
            && !(managed.membership_may_have_changed
                && (observed.voter_sets == target || observed.voter_sets == joint))
        {
            bail!(
                "group {} configuration differs from its active intent",
                observed.raft_group_id.0
            );
        }
        if !observed.learners.is_subset(&migration.added_nodes)
            || !observed.learners.is_subset(&managed.prepared)
        {
            bail!("unprepared or unrelated learners in managed configuration");
        }
    } else if observed.voter_sets != vec![placement.voters.clone()] || !observed.learners.is_empty()
    {
        bail!(
            "group {} configuration differs from settled placement",
            observed.raft_group_id.0
        );
    }
    for (id, endpoint) in &observed.nodes {
        let node = view
            .state
            .nodes
            .get(id)
            .context("configuration names an unregistered node")?;
        if !matches!(node.state, NodeState::Active | NodeState::Draining)
            || url::Url::parse(endpoint)? != url::Url::parse(&node.cluster_url)?
        {
            bail!("configuration origin/state differs from trusted node {id}");
        }
    }
    Ok(())
}

fn quorum_coverage(
    sets: &[BTreeSet<u64>],
    observed: &BTreeSet<u64>,
) -> Result<(Vec<usize>, Vec<usize>)> {
    let required: Vec<_> = sets.iter().map(|set| set.len() / 2 + 1).collect();
    let counts: Vec<_> = sets
        .iter()
        .map(|set| set.intersection(observed).count())
        .collect();
    if sets.is_empty()
        || sets.iter().any(BTreeSet::is_empty)
        || counts
            .iter()
            .zip(&required)
            .any(|(count, required)| count < required)
    {
        bail!("observed voters cannot satisfy every constituent group quorum");
    }
    Ok((required, counts))
}

async fn configuration(
    view: &ControlProjection,
    group: RaftGroupId,
    timeout: Duration,
) -> Result<CommittedGroupConfiguration> {
    let placement = view
        .state
        .placements
        .get(&group)
        .context("group placement is missing")?;
    let candidates: BTreeSet<_> = if let Some(migration) = view
        .state
        .active_migration()
        .filter(|migration| migration.raft_group_id == group)
    {
        migration
            .from_voters
            .union(&migration.target_voters)
            .copied()
            .collect()
    } else {
        placement.voters.clone()
    };
    let requests = candidates.iter().map(|id| async move {
        let node = view
            .state
            .nodes
            .get(id)
            .context("unregistered quorum candidate")?;
        ursula_raft::confirm_group_configuration(group, *id, &node.cluster_url, timeout)
            .await
            .map_err(anyhow::Error::msg)
    });
    let mut requests = futures_util::stream::iter(requests).buffer_unordered(5);
    let mut last = anyhow::anyhow!("no data leader quorum for group {}", group.0);
    while let Some(result) = requests.next().await {
        match result {
            Ok(observed) => {
                validate_configuration(view, &observed)?;
                return Ok(observed);
            }
            Err(error) => last = error,
        }
    }
    Err(last)
}

fn same_configuration(
    before: &CommittedGroupConfiguration,
    after: &CommittedGroupConfiguration,
) -> bool {
    before.raft_group_id == after.raft_group_id
        && before.leader_id == after.leader_id
        && before.leader_term == after.leader_term
        && before.membership_log_id == after.membership_log_id
        && before.voter_sets == after.voter_sets
        && before.learners == after.learners
        && before.nodes == after.nodes
        && after.applied_log_id.index >= before.applied_log_id.index
}

fn validate_inventory(
    view: &ControlProjection,
    snapshot: &ClusterSnapshot,
) -> Result<Option<bool>> {
    let mut eligible = true;
    for node in &snapshot.per_node {
        let report = node
            .raft_maintenance
            .as_ref()
            .context("managed quorum requires assignment-aware inventory")?;
        let inventory = report
            .managed_inventory
            .as_ref()
            .context("legacy maintenance inventory cannot certify managed participation")?;
        if report.version != 3
            || report.node_id != node.node.id
            || node.process_incarnation.is_none()
        {
            bail!(
                "managed node {} has stale or uncertain assignment inventory",
                node.node.id
            );
        }
        if inventory.applied_meta_index < view.applied_log_id.index {
            return Ok(None);
        }
        if inventory.assignment_drift {
            bail!(
                "managed node {} assignment ledger differs from placement/intent",
                node.node.id
            );
        }
        let expected: BTreeSet<_> = view
            .state
            .placements
            .iter()
            .filter(|(_, placement)| placement.voters.contains(&node.node.id))
            .map(|(group, _)| group.0)
            .collect();
        let reported: BTreeSet<_> = report.expected_groups.keys().copied().collect();
        let resident: BTreeSet<_> = node
            .groups
            .iter()
            .map(|group| u32::try_from(group.raft_group_id))
            .collect::<std::result::Result<_, _>>()?;
        if !expected.is_subset(&reported)
            || resident != reported
            || node.groups.len() != resident.len()
            || !inventory
                .replica_roles
                .keys()
                .eq(report.expected_groups.keys())
        {
            bail!(
                "managed node {} replica inventory is incomplete",
                node.node.id
            );
        }
        for (group, voters) in &report.expected_groups {
            let placement = view
                .state
                .placements
                .get(&RaftGroupId(*group))
                .context("unexpected resident group")?;
            if *voters != placement.voters
                || (!expected.contains(group)
                    && !view.state.active_migration().is_some_and(|migration| {
                        migration.raft_group_id.0 == *group
                            && (migration.from_voters.contains(&node.node.id)
                                || migration.target_voters.contains(&node.node.id))
                    }))
            {
                bail!(
                    "managed node {} reports an unauthorized replica",
                    node.node.id
                );
            }
        }
        eligible &= report.ready();
    }
    Ok(Some(eligible))
}

/// Read complete fresh control state, capture one native quorum-confirmed
/// configuration per group, then check fixed-prefix application only on its
/// selected voters. RF5 survivors and joint sets preserve their original
/// denominators. Final native/meta reads refuse observations spanning drift.
pub async fn verify_managed_quorum(
    seeds: &[NodeInfo],
    client: &MetricsClient,
    options: &ManagedQuorumOptions,
) -> Result<ManagedQuorumVerification> {
    if options.timeout.is_zero() || options.poll_interval.is_zero() || client.timeout().is_zero() {
        bail!("managed quorum timeouts and poll interval must be nonzero");
    }
    tokio::time::timeout(options.timeout, async {
        let view = OperationClient::new(client.timeout())?.list(seeds).await?;
        let bootstrap = view.state.cluster_bootstrap.as_ref().context("managed bootstrap is missing")?;
        if !options.excluded_nodes.is_subset(&view.state.nodes.keys().copied().collect()) { bail!("excluded node is not registered"); }
        let mut configurations = BTreeMap::new();
        for group in view.state.placements.keys() {
            configurations.insert(group.0, configuration(&view, *group, client.timeout()).await?);
        }
        let required: BTreeSet<_> = configurations.values().flat_map(|configuration| configuration.voter_sets.iter().flatten()).copied().filter(|id| !options.excluded_nodes.contains(id)).collect();
        for config in configurations.values() {
            quorum_coverage(&config.voter_sets, &required)?;
        }
        for config in configurations.values() {
            if !required.contains(&config.leader_id) { bail!("current data leader is excluded; resample after leadership changes"); }
        }
        let nodes = required.iter().map(|id| {
            let node = view.state.nodes.get(id).context("unregistered observed voter")?;
            let admin_url: url::Url = node.admin_url.as_ref().context("registered voter lacks admin origin")?.parse()?;
            Ok(NodeInfo { id: *id, metrics_url: Some(admin_url.clone()), admin_url, host: node.client_url.clone(), http_url: Some(node.cluster_url.parse()?), expected_process_incarnation: None, expected_maintenance_fence: None })
        }).collect::<Result<Vec<_>>>()?;
        loop {
            let snapshot = client.fetch_cluster(&nodes).await?;
            let Some(eligible) = validate_inventory(&view, &snapshot)? else {
                tokio::time::sleep(options.poll_interval).await;
                continue;
            };
            let mut groups = BTreeMap::new();
            let mut caught_up = true;
            for (group, config) in &configurations {
                let voters: BTreeSet<_> = config.voter_sets.iter().flatten().copied().collect();
                let observed_voters: BTreeSet<_> = voters.intersection(&required).copied().collect();
                let (required_majorities, observed_per_set) = quorum_coverage(&config.voter_sets, &observed_voters)?;
                let mut applied = BTreeMap::new();
                for id in &observed_voters {
                    let replica = snapshot.per_node.iter().find(|node| node.node.id == *id).and_then(|node| node.group(u64::from(*group))).context("observed voter lacks its assigned group")?;
                    let state = replica.maintenance.as_ref().context("replica lacks maintenance capability")?;
                    if state.membership_log_index.is_none_or(|index| index < config.membership_log_id.index) {
                        caught_up = false;
                        continue;
                    }
                    if !state.running || !state.recovery_ready || state.stopped_for_operator || replica.current_term != Some(config.leader_term) || replica.current_leader != Some(config.leader_id) {
                        bail!("group {group} replica {id} no longer participates under the captured leader");
                    }
                    if replica.voter_ids.iter().copied().collect::<BTreeSet<_>>() != voters || replica.voter_ids.len() != voters.len()
                        || replica.learner_ids.iter().copied().collect::<BTreeSet<_>>() != config.learners
                        || replica.learner_ids.len() != config.learners.len()
                        || state.membership_joint != (config.voter_sets.len() == 2)
                        || state.membership_log_index != Some(config.membership_log_id.index) {
                        bail!("group {group} replica {id} membership differs from the captured configuration");
                    }
                    let index = replica.last_applied_index.context("replica has no applied prefix")?;
                    caught_up &= index >= config.applied_log_id.index;
                    applied.insert(*id, index);
                }
                groups.insert(*group, ManagedGroupQuorum { configuration: config.clone(), full_redundancy_observed: observed_voters == voters, observed_voters, required_majorities, observed_per_set, applied });
            }
            if caught_up {
                for (group, before) in &configurations {
                    let after = configuration(&view, RaftGroupId(*group), client.timeout()).await?;
                    if !same_configuration(before, &after) { bail!("data configuration/leader changed; resample managed quorum evidence"); }
                }
                let mut final_view = None;
                for id in &bootstrap.recipe.initial_meta_voters {
                    let node = bootstrap.recipe.nodes.get(id).context("bootstrap meta origin is missing")?;
                    if let Ok(fresh) = ursula_raft::read_control_projection(&bootstrap.recipe.identity, *id, &node.cluster_url, client.timeout()).await { final_view = Some(fresh); break; }
                }
                let fresh = final_view.context("no final fresh meta quorum")?;
                if fresh.identity != view.identity || fresh.state != view.state { bail!("control topology/intent changed; resample managed quorum evidence"); }
                return Ok(ManagedQuorumVerification {
                    version: 1, applied_meta_index: view.applied_log_id.index,
                    active_migration_id: view.state.active_migration().map(|migration| migration.migration_id),
                    excluded_nodes: options.excluded_nodes.clone(),
                    process_incarnations: snapshot.per_node.iter().map(|node| Ok((node.node.id, node.process_incarnation.clone().context("missing process identity")?))).collect::<Result<_>>()?,
                    maintenance_eligible: eligible && view.state.active_migration().is_none() && groups.values().all(|group| group.full_redundancy_observed && group.configuration.voter_sets.len() == 1 && group.configuration.learners.is_empty()),
                    disruption_authorized: false, groups,
                });
            }
            tokio::time::sleep(options.poll_interval).await;
        }
    }).await.context("managed quorum verification deadline reached")?
}

#[cfg(test)]
mod tests {
    use super::quorum_coverage;
    #[test]
    fn rf5_and_joint_observations_keep_every_constituent_denominator() {
        assert_eq!(
            quorum_coverage(&[[1, 2, 3, 4, 5].into()], &[1, 2, 3].into()).unwrap(),
            (vec![3], vec![3])
        );
        assert!(quorum_coverage(&[[1, 2, 3, 4, 5].into()], &[1, 2].into()).is_err());
        let joint = vec![[1, 2, 3].into(), [3, 4, 5].into()];
        assert!(quorum_coverage(&joint, &[1, 2, 3].into()).is_err());
        assert_eq!(
            quorum_coverage(&joint, &[1, 3, 4].into()).unwrap(),
            (vec![2, 2], vec![2, 2])
        );
    }
}
