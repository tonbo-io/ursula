//! Pre-fault host identities retained in the same whole-object CAS as maintenance.
//! A healthy inventory observation grants no host or Pod disruption authority.

use std::collections::BTreeSet;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use super::CellIdentity;
use super::PrefixObservation;
use super::Reservation;
use super::SourceIdentity;
use super::fresh;
use super::identity;
use super::no_regression;
use super::validate_observed_prefix;
use super::validate_processes;
use crate::NodeInfo;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostVoter {
    pub source: SourceIdentity,
    pub node_name: String,
    pub failure_domain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostInventory {
    pub voters: Vec<HostVoter>,
    pub process_plan: Vec<NodeInfo>,
    pub observation: PrefixObservation,
}

/// JSON's ordinary struct deserializer retains integer map keys in the proof.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "action", rename = "publish_host_inventory", deny_unknown_fields)]
pub struct PublishHostInventory {
    pub now_ms: u64,
    pub pods: Vec<Value>,
    pub nodes: Vec<Value>,
    pub process_plan: Vec<NodeInfo>,
    pub observation: PrefixObservation,
}

impl<'de> Deserialize<'de> for PublishHostInventory {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            now_ms: u64,
            pods: Vec<Value>,
            nodes: Vec<Value>,
            process_plan: Vec<NodeInfo>,
            observation: PrefixObservation,
        }
        let mut value = Value::deserialize(deserializer)?;
        let action = value
            .as_object_mut()
            .and_then(|object| object.remove("action"));
        if action.as_ref().and_then(Value::as_str) != Some("publish_host_inventory") {
            return Err(<D::Error as serde::de::Error>::custom(
                "unsupported host inventory action",
            ));
        }
        let fields: Fields =
            serde_json::from_value(value).map_err(<D::Error as serde::de::Error>::custom)?;
        Ok(Self {
            now_ms: fields.now_ms,
            pods: fields.pods,
            nodes: fields.nodes,
            process_plan: fields.process_plan,
            observation: fields.observation,
        })
    }
}

pub(super) fn ready(object: &Value, kind: &str) -> Result<()> {
    if !object
        .pointer("/status/conditions")
        .and_then(Value::as_array)
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(Value::as_str) == Some("Ready")
                    && condition.get("status").and_then(Value::as_str) == Some("True")
            })
        })
    {
        bail!("{kind} must be Ready when publishing a healthy host inventory");
    }
    Ok(())
}

impl HostVoter {
    pub(super) fn capture(
        cell: &CellIdentity,
        source: SourceIdentity,
        node: &Value,
    ) -> Result<Self> {
        source.validate(cell)?;
        if node.get("kind").and_then(Value::as_str) != Some("Node")
            || node.pointer("/metadata/uid").and_then(Value::as_str)
                != Some(source.node_uid.as_str())
            || node.pointer("/spec/providerID").and_then(Value::as_str)
                != Some(source.provider_instance.as_str())
            || node
                .pointer("/metadata/deletionTimestamp")
                .is_some_and(|value| !value.is_null())
        {
            bail!("host observation differs from the captured source Node");
        }
        let host = Self {
            source,
            node_name: node
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .context("missing Node name")?
                .to_owned(),
            failure_domain: node
                .pointer("/metadata/labels/topology.kubernetes.io~1zone")
                .and_then(Value::as_str)
                .context("missing Node failure domain")?
                .to_owned(),
        };
        host.validate(cell)?;
        Ok(host)
    }

    pub(super) fn validate(&self, cell: &CellIdentity) -> Result<()> {
        self.source.validate(cell)?;
        identity(&self.node_name, "Node name")?;
        identity(&self.failure_domain, "failure domain")
    }

    pub(super) fn same_host(&self, other: &Self) -> bool {
        self.source.node_id == other.source.node_id
            && self.source.node_uid == other.source.node_uid
            && self.source.provider_instance == other.source.provider_instance
            && self.node_name == other.node_name
            && self.failure_domain == other.failure_domain
    }
}

impl HostInventory {
    pub fn voter(&self, node_id: u64) -> Result<&HostVoter> {
        self.voters
            .iter()
            .find(|voter| voter.source.node_id == node_id)
            .context("missing catalogued voter")
    }

    pub(super) fn validate(&self, cell: &CellIdentity) -> Result<()> {
        if self.voters.len() != cell.voter_ids.len()
            || self
                .voters
                .iter()
                .map(|voter| voter.source.node_id)
                .collect::<BTreeSet<_>>()
                != cell.voter_ids
            || self
                .process_plan
                .iter()
                .map(|node| node.id)
                .collect::<BTreeSet<_>>()
                != cell.voter_ids
            || self
                .process_plan
                .iter()
                .any(|node| node.expected_maintenance_fence.is_some())
        {
            bail!(
                "healthy host inventory requires every configured voter and no active executor plan"
            );
        }
        for voter in &self.voters {
            voter.validate(cell)?;
            if self
                .observation
                .verification
                .process_incarnations
                .get(&voter.source.node_id)
                != Some(&voter.source.process_incarnation)
            {
                bail!("host identity and Raft process proof differ");
            }
        }
        for unique in [
            self.voters
                .iter()
                .map(|voter| &voter.source.pod_uid)
                .collect::<BTreeSet<_>>(),
            self.voters
                .iter()
                .map(|voter| &voter.source.node_uid)
                .collect(),
            self.voters
                .iter()
                .map(|voter| &voter.source.provider_instance)
                .collect(),
            self.voters.iter().map(|voter| &voter.node_name).collect(),
            self.voters
                .iter()
                .map(|voter| &voter.failure_domain)
                .collect(),
        ] {
            if unique.len() != cell.voter_ids.len() {
                bail!(
                    "voters must have distinct Pods, hosts, provider identities and failure domains"
                );
            }
        }
        validate_observed_prefix(cell, &cell.voter_ids, &self.observation)?;
        validate_processes(&self.process_plan, &self.observation)?;
        let proof = &self.observation.verification;
        if proof.maintenance_executor_certified
            || proof.maintenance_executor_retired_certified != proof.maintenance_fence.is_some()
        {
            bail!("healthy inventory cannot retain an active or uncertified executor");
        }
        Ok(())
    }

    pub(super) fn validate_source(
        &self,
        source: &SourceIdentity,
        plan: &[NodeInfo],
        now_ms: u64,
    ) -> Result<()> {
        if &self.voter(source.node_id)?.source != source || now_ms < self.observation.completed_ms {
            bail!("maintenance source differs from the retained healthy inventory");
        }
        if plan.len() != self.process_plan.len() {
            bail!("maintenance changed the catalogued process inventory");
        }
        for saved in &self.process_plan {
            let candidate = plan
                .iter()
                .find(|node| node.id == saved.id)
                .context("maintenance lost a catalogued voter")?;
            let mut candidate = candidate.clone();
            candidate.expected_maintenance_fence = None;
            if serde_json::to_value(saved)? != serde_json::to_value(candidate)? {
                bail!("maintenance process plan differs from the healthy inventory");
            }
        }
        Ok(())
    }

    pub(super) fn complete_pod_replacement(
        &mut self,
        replacement: &SourceIdentity,
        plan: &[NodeInfo],
        observation: &PrefixObservation,
    ) -> Result<()> {
        let mut selected = self.voter(replacement.node_id)?.clone();
        if selected.source.node_uid != replacement.node_uid
            || selected.source.provider_instance != replacement.provider_instance
        {
            bail!("Pod completion cannot overwrite the pre-fault physical host");
        }
        selected.source = replacement.clone();
        self.complete_host_replacement(selected, plan, observation)
    }

    pub(super) fn complete_host_replacement(
        &mut self,
        replacement: HostVoter,
        plan: &[NodeInfo],
        observation: &PrefixObservation,
    ) -> Result<()> {
        no_regression(&self.observation, observation)?;
        let selected = self
            .voters
            .iter_mut()
            .find(|voter| voter.source.node_id == replacement.source.node_id)
            .context("replacement lost the catalogued source")?;
        *selected = replacement;
        self.process_plan = plan.to_vec();
        for node in &mut self.process_plan {
            node.expected_maintenance_fence = None;
        }
        self.observation = observation.clone();
        Ok(())
    }
}

impl Reservation {
    /// Idle-only schema migration/refresh, acquired by the existing whole-store CAS.
    /// Healthy refresh may update process/Pod incarnations on the same physical
    /// hosts; a changed host requires the fenced replacement state machine.
    pub fn publish_hosts(&self, request: PublishHostInventory) -> Result<Self> {
        self.validate()?;
        if self.operation.is_some() {
            bail!("host inventory cannot change during a reserved operation");
        }
        if request.pods.len() != self.cell.voter_ids.len()
            || request.nodes.len() != self.cell.voter_ids.len()
        {
            bail!("host capture requires complete Pod and Node inventories");
        }
        fresh(&request.observation, request.now_ms, 1)?;
        let mut voters = Vec::new();
        for node_id in &self.cell.voter_ids {
            let ordinal = node_id.checked_sub(1).context("invalid voter")?;
            let name = format!("{}-{ordinal}", self.cell.statefulset);
            let pod = request
                .pods
                .iter()
                .find(|pod| {
                    pod.pointer("/metadata/name").and_then(Value::as_str) == Some(name.as_str())
                })
                .context("missing voter Pod")?;
            let node_name = pod
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .context("Pod is not assigned to a host")?;
            let node = request
                .nodes
                .iter()
                .find(|node| {
                    node.pointer("/metadata/name").and_then(Value::as_str) == Some(node_name)
                })
                .context("missing voter Node")?;
            ready(pod, "Pod")?;
            ready(node, "Node")?;
            let source =
                SourceIdentity::capture(&self.cell, *node_id, pod, node, &request.process_plan)?;
            voters.push(HostVoter::capture(&self.cell, source, node)?);
        }
        let hosts = HostInventory {
            voters,
            process_plan: request.process_plan,
            observation: request.observation,
        };
        hosts.validate(&self.cell)?;
        if let Some(receipt) = &self.completion {
            no_regression(&receipt.observation, &hosts.observation)?;
            if hosts.observation.started_ms < receipt.observation.completed_ms {
                bail!("host capture predates previous maintenance completion");
            }
        }
        if hosts.observation.verification.maintenance_fence.is_some()
            && self.completion.as_ref().map(|receipt| &receipt.fence)
                != hosts.observation.verification.maintenance_fence.as_ref()
        {
            bail!("inventory retirement proof belongs to another operation");
        }
        if let Some(previous) = &self.hosts {
            no_regression(&previous.observation, &hosts.observation)?;
            if hosts.observation.started_ms < previous.observation.completed_ms {
                bail!("host inventory observation regressed");
            }
            for voter in &hosts.voters {
                if !previous.voter(voter.source.node_id)?.same_host(voter) {
                    bail!(
                        "healthy refresh cannot discard a physical host that has not been fenced"
                    );
                }
            }
        }
        let mut next = self.clone();
        next.version = self.version.max(2);
        next.hosts = Some(hosts);
        next.validate()?;
        Ok(next)
    }
}
