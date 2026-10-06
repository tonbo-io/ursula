//! Shared maintenance ownership, independent of ephemeral platform executors.
//!
//! This module proposes whole-object CAS updates; it performs no Kubernetes
//! or host operations. A proposal is not acquired authority. Platform consumers
//! must submit it using the retained UID/resourceVersion and validate the API
//! acknowledgement. Admission, retirement and physical reconciliation are
//! additional gates; ownership alone never permits disruption or release.

use std::collections::BTreeSet;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use ursula_proto::admin::MaintenanceFence;
use ursula_proto::admin::ProcessIncarnation;

use crate::NodeInfo;
use crate::quorum::QuorumVerification;

mod hosts;
mod inventory;
mod recovery;
mod startup;
mod store;
#[cfg(test)]
pub(crate) mod tests;

pub use hosts::HostInventory;
pub use hosts::HostVoter;
pub use hosts::PublishHostInventory;
pub use recovery::HostRecovery;
pub use recovery::HostRequest;
pub use recovery::HostTerminationObservation;
pub use recovery::ReplacementRetirement;
pub use recovery::RetiredHostReplacement;
pub use recovery::SurvivingPrefixObservation;
pub use store::CasProposal;
pub use store::ConfigMapSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellIdentity {
    pub namespace: String,
    pub namespace_uid: String,
    pub statefulset: String,
    pub statefulset_uid: String,
    pub group_count: u32,
    pub core_count: u16,
    pub voter_ids: BTreeSet<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIdentity {
    pub node_id: u64,
    pub pod_name: String,
    pub pod_uid: String,
    pub node_uid: String,
    pub provider_instance: String,
    pub process_incarnation: ProcessIncarnation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub fence: MaintenanceFence,
    pub source: SourceIdentity,
    pub process_plan: Vec<NodeInfo>,
    pub acquired_ms: u64,
    pub admission: Option<PrefixObservation>,
    pub replacement: Option<SourceIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<HostRecovery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    version: u32,
    cell: CellIdentity,
    generation: u64,
    operation: Option<Operation>,
    completion: Option<Completion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hosts: Option<HostInventory>,
}

/// A CLI verification observation; not authentication or provider fencing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrefixObservation {
    pub started_ms: u64,
    pub completed_ms: u64,
    pub verification: QuorumVerification,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub fence: MaintenanceFence,
    pub source: SourceIdentity,
    pub replacement: SourceIdentity,
    pub observation: PrefixObservation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<HostRecovery>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProgressRequest {
    AdmitPodDeletion {
        fence: MaintenanceFence,
        now_ms: u64,
        observation: PrefixObservation,
    },
    BindPodReplacement {
        fence: MaintenanceFence,
        pod: Value,
        node: Value,
        process_plan: Vec<NodeInfo>,
    },
    CompletePodReplacement {
        fence: MaintenanceFence,
        now_ms: u64,
        observation: PrefixObservation,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnershipRequest {
    Reserve {
        operation_id: String,
        executor_id: String,
        source: SourceIdentity,
        process_plan: Vec<NodeInfo>,
        now_ms: u64,
    },
    Takeover {
        operation_id: String,
        executor_id: String,
        now_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ReservationRequest {
    Ownership(OwnershipRequest),
    Progress(ProgressRequest),
    Inventory(PublishHostInventory),
    Host(HostRequest),
}

// Serde's buffered internally tagged/untagged deserializers cannot parse
// JSON's string-encoded integer map keys in the quorum evidence. Dispatch the
// action explicitly and deserialize each plain object using serde_json, which
// retains its JSON map-key semantics.
impl<'de> Deserialize<'de> for ProgressRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ObservationFields {
            fence: MaintenanceFence,
            now_ms: u64,
            observation: PrefixObservation,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct BindingFields {
            fence: MaintenanceFence,
            pod: Value,
            node: Value,
            process_plan: Vec<NodeInfo>,
        }
        let mut value = Value::deserialize(deserializer)?;
        let action = value
            .as_object_mut()
            .and_then(|object| object.remove("action"))
            .and_then(|action| action.as_str().map(str::to_owned))
            .ok_or_else(|| <D::Error as serde::de::Error>::custom("missing progress action"))?;
        let parse_error = <D::Error as serde::de::Error>::custom;
        match action.as_str() {
            "admit_pod_deletion" | "complete_pod_replacement" => {
                let fields: ObservationFields =
                    serde_json::from_value(value).map_err(parse_error)?;
                if action == "admit_pod_deletion" {
                    Ok(Self::AdmitPodDeletion {
                        fence: fields.fence,
                        now_ms: fields.now_ms,
                        observation: fields.observation,
                    })
                } else {
                    Ok(Self::CompletePodReplacement {
                        fence: fields.fence,
                        now_ms: fields.now_ms,
                        observation: fields.observation,
                    })
                }
            }
            "bind_pod_replacement" => {
                let fields: BindingFields = serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::BindPodReplacement {
                    fence: fields.fence,
                    pod: fields.pod,
                    node: fields.node,
                    process_plan: fields.process_plan,
                })
            }
            _ => Err(<D::Error as serde::de::Error>::custom(
                "unsupported Pod-replacement progress action",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for ReservationRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let parse_error = <D::Error as serde::de::Error>::custom;
        match value.get("action").and_then(Value::as_str) {
            Some("reserve" | "takeover") => serde_json::from_value(value)
                .map(Self::Ownership)
                .map_err(parse_error),
            Some("admit_pod_deletion" | "bind_pod_replacement" | "complete_pod_replacement") => {
                serde_json::from_value(value)
                    .map(Self::Progress)
                    .map_err(parse_error)
            }
            Some("publish_host_inventory") => serde_json::from_value(value)
                .map(Self::Inventory)
                .map_err(parse_error),
            Some(
                "reserve_host_recovery"
                | "admit_host_termination"
                | "record_host_termination"
                | "admit_fenced_pod_retirement"
                | "bind_host_replacement"
                | "complete_host_replacement"
                | "admit_replacement_termination"
                | "record_replacement_termination"
                | "restage_host_replacement",
            ) => serde_json::from_value(value)
                .map(Self::Host)
                .map_err(parse_error),
            _ => Err(<D::Error as serde::de::Error>::custom(
                "unsupported reservation action",
            )),
        }
    }
}

fn identity(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        bail!("{label} must be a bounded nonempty opaque identity");
    }
    Ok(())
}

impl SourceIdentity {
    fn validate(&self, cell: &CellIdentity) -> Result<()> {
        if !cell.voter_ids.contains(&self.node_id) {
            bail!("source voter is outside cell inventory");
        }
        let ordinal = self
            .node_id
            .checked_sub(1)
            .context("invalid source voter")?;
        if self.pod_name != format!("{}-{ordinal}", cell.statefulset) {
            bail!("selected Pod name does not match its voter ordinal");
        }
        for (value, label) in [
            (&self.pod_uid, "Pod UID"),
            (&self.node_uid, "Node UID"),
            (&self.provider_instance, "provider identity"),
        ] {
            identity(value, label)?;
        }
        Ok(())
    }
}

impl Reservation {
    /// Reviewed bootstrap only. Missing/deleted stores must not call this as
    /// an automatic recovery fallback; this module never initializes a store.
    pub fn initial(cell: CellIdentity) -> Result<Self> {
        let state = Self {
            version: 1,
            cell,
            generation: 0,
            operation: None,
            completion: None,
            hosts: None,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn cell(&self) -> &CellIdentity {
        &self.cell
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn completion(&self) -> Option<&Completion> {
        self.completion.as_ref()
    }

    pub fn operation(&self) -> Option<&Operation> {
        self.operation.as_ref()
    }

    pub fn hosts(&self) -> Option<&HostInventory> {
        self.hosts.as_ref()
    }

    pub fn validate(&self) -> Result<()> {
        if !matches!(self.version, 1..=4)
            || self.cell.group_count == 0
            || self.cell.core_count == 0
            || self.cell.voter_ids != BTreeSet::from([1, 2, 3])
        {
            bail!("unsupported reservation schema or three-voter inventory");
        }
        if (self.version >= 2) != self.hosts.is_some() {
            bail!("host inventory requires an explicit schema-2 CAS migration");
        }
        if let Some(hosts) = &self.hosts {
            hosts.validate(&self.cell)?;
            if let Some(receipt) = &self.completion {
                no_regression(&receipt.observation, &hosts.observation)?;
                if hosts.observation.started_ms < receipt.observation.completed_ms
                    && serde_json::to_value(&hosts.observation)?
                        != serde_json::to_value(&receipt.observation)?
                {
                    bail!("host inventory predates previous completion");
                }
            }
            if hosts.observation.verification.maintenance_fence.is_some()
                && self.completion.as_ref().map(|receipt| &receipt.fence)
                    != hosts.observation.verification.maintenance_fence.as_ref()
            {
                bail!("host inventory has no matching executor retirement receipt");
            }
        }
        for (value, label) in [
            (&self.cell.namespace, "namespace"),
            (&self.cell.namespace_uid, "namespace UID"),
            (&self.cell.statefulset, "StatefulSet"),
            (&self.cell.statefulset_uid, "StatefulSet UID"),
        ] {
            identity(value, label)?;
        }
        if self.operation.is_none() && self.generation != 0 && self.completion.is_none() {
            bail!("noninitial idle state lacks a reviewed completion receipt");
        }
        if let Some(receipt) = &self.completion {
            receipt.source.validate(&self.cell)?;
            receipt.replacement.validate(&self.cell)?;
            if self.operation.is_none() && receipt.fence.generation() != self.generation {
                bail!("idle generation differs from completion receipt");
            }
            if receipt.source.node_id != receipt.replacement.node_id
                || receipt.source.pod_uid == receipt.replacement.pod_uid
                || receipt.source.process_incarnation == receipt.replacement.process_incarnation
            {
                bail!("completion lacks an irreversible source Pod replacement");
            }
            validate_prefix(&self.cell, &receipt.fence, &receipt.observation, true)?;
            if receipt.fence.generation() > self.generation
                || receipt
                    .observation
                    .verification
                    .process_incarnations
                    .get(&receipt.replacement.node_id)
                    != Some(&receipt.replacement.process_incarnation)
            {
                bail!("completion process or generation does not match its replacement");
            }
            if let Some(host) = &receipt.host {
                if self.version < 3 || (host.uses_restage_schema() && self.version != 4) {
                    bail!("host completion requires schema 3, restaging requires schema 4");
                }
                host.validate_completion(&self.cell, receipt)?;
                host.validate_retired_host_placement(
                    self.hosts.as_ref().context("no host inventory")?,
                )?;
            }
        }
        if let Some(operation) = &self.operation {
            if operation.fence.generation() != self.generation
                || !self.cell.voter_ids.contains(&operation.source.node_id)
                || operation.process_plan.len() != self.cell.voter_ids.len()
            {
                bail!("reservation generation, target or inventory changed");
            }
            operation.source.validate(&self.cell)?;
            if let Some(replacement) = &operation.replacement {
                replacement.validate(&self.cell)?;
            }
            let ids = operation
                .process_plan
                .iter()
                .map(|node| node.id)
                .collect::<BTreeSet<_>>();
            if ids != self.cell.voter_ids {
                bail!("process plan differs from the original voter inventory");
            }
            for node in &operation.process_plan {
                if node.expected_process_incarnation.is_none()
                    || node.expected_maintenance_fence.as_ref() != Some(&operation.fence)
                {
                    bail!("each voter requires fixed process and executor identities");
                }
            }
            let target = operation
                .process_plan
                .iter()
                .find(|node| node.id == operation.source.node_id)
                .context("missing selected source")?;
            let bound_source = operation.replacement.as_ref().unwrap_or(&operation.source);
            if let Some(hosts) = &self.hosts {
                let mut original_plan = operation.process_plan.clone();
                let original = original_plan
                    .iter_mut()
                    .find(|node| node.id == operation.source.node_id)
                    .context("missing original catalogued voter")?;
                original.expected_process_incarnation =
                    Some(operation.source.process_incarnation.clone());
                if let Some(host) = &operation.host {
                    if self.version < 3 || (host.uses_restage_schema() && self.version != 4) {
                        bail!("host recovery requires schema 3, restaging requires schema 4");
                    }
                    host.validate_operation(self, operation)?;
                } else {
                    hosts.validate_source(
                        &operation.source,
                        &original_plan,
                        operation.acquired_ms,
                    )?;
                }
            }
            if operation.replacement.as_ref().is_some_and(|replacement| {
                replacement.node_id != operation.source.node_id
                    || replacement.pod_name != operation.source.pod_name
                    || replacement.pod_uid == operation.source.pod_uid
                    || replacement.process_incarnation == operation.source.process_incarnation
            }) || operation.acquired_ms == 0
            {
                bail!("invalid physical replacement or acquisition timestamp");
            }
            if operation.host.is_some() && operation.admission.is_some() {
                bail!("host recovery cannot have planned Pod deletion admission");
            }
            if operation.host.is_none()
                && operation.replacement.is_some()
                && operation.admission.is_none()
            {
                bail!("replacement lacks persistent deletion admission");
            }
            if let Some(admission) = &operation.admission {
                let original_fence = admission
                    .verification
                    .maintenance_fence
                    .as_ref()
                    .context("missing admission token")?;
                if original_fence.reservation_id() != operation.fence.reservation_id()
                    || original_fence.generation() > operation.fence.generation()
                {
                    bail!("admission belongs to another operation");
                }
                validate_prefix(&self.cell, original_fence, admission, false)?;
                let mut original_plan = operation.process_plan.clone();
                let original_target = original_plan
                    .iter_mut()
                    .find(|node| node.id == operation.source.node_id)
                    .context("missing original target")?;
                original_target.expected_process_incarnation =
                    Some(operation.source.process_incarnation.clone());
                validate_processes(&original_plan, admission)?;
                if let Some(hosts) = &self.hosts {
                    no_regression(&hosts.observation, admission)?;
                }
            }
            if target.expected_process_incarnation.as_ref()
                != Some(&bound_source.process_incarnation)
            {
                bail!("selected source process differs from the pinned plan");
            }
        }
        Ok(())
    }

    /// Ownership only: no expiry, release, target refresh or disruption grant.
    pub fn propose(&self, request: OwnershipRequest) -> Result<Self> {
        self.validate()?;
        let mut next = self.clone();
        next.generation = self
            .generation
            .checked_add(1)
            .context("maintenance generation exhausted")?;
        match request {
            OwnershipRequest::Reserve {
                operation_id,
                executor_id,
                source,
                mut process_plan,
                now_ms,
            } => {
                if self.operation.is_some() {
                    bail!("another source remains reserved; only that operation may be resumed");
                }
                if let Some(receipt) = &self.completion {
                    if self.hosts.is_none() {
                        validate_processes(&process_plan, &receipt.observation)?;
                    }
                    if self.hosts.is_none()
                        && source.node_id == receipt.replacement.node_id
                        && source != receipt.replacement
                    {
                        bail!("selected source no longer matches the last replacement identity");
                    }
                    if now_ms < receipt.observation.completed_ms {
                        bail!("next operation predates previous completion");
                    }
                }
                if let Some(hosts) = &self.hosts {
                    hosts.validate_source(&source, &process_plan, now_ms)?;
                }
                let fence = MaintenanceFence::new(operation_id, executor_id, next.generation)
                    .map_err(anyhow::Error::msg)?;
                for node in &mut process_plan {
                    node.expected_maintenance_fence = Some(fence.clone());
                }
                next.operation = Some(Operation {
                    fence,
                    source,
                    process_plan,
                    acquired_ms: now_ms,
                    admission: None,
                    replacement: None,
                    host: None,
                });
            }
            OwnershipRequest::Takeover {
                operation_id,
                executor_id,
                now_ms,
            } => {
                let operation = next
                    .operation
                    .as_mut()
                    .context("no admitted operation to resume")?;
                if operation.fence.reservation_id() != operation_id
                    || operation.fence.executor_id() == executor_id
                {
                    bail!("takeover requires the same operation and a different executor identity");
                }
                if now_ms < operation.acquired_ms {
                    bail!("takeover clock regressed");
                }
                operation.acquired_ms = now_ms;
                // A physical replacement is sticky. Takeover must recertify the
                // current plan before proceeding, and cannot select another UID.
                operation.fence = MaintenanceFence::new(operation_id, executor_id, next.generation)
                    .map_err(anyhow::Error::msg)?;
                for node in &mut operation.process_plan {
                    node.expected_maintenance_fence = Some(operation.fence.clone());
                }
            }
        }
        next.validate()?;
        Ok(next)
    }
}

fn validate_prefix(
    cell: &CellIdentity,
    fence: &MaintenanceFence,
    observation: &PrefixObservation,
    retired: bool,
) -> Result<()> {
    validate_observed_prefix(cell, &cell.voter_ids, observation)?;
    let proof = &observation.verification;
    if proof.maintenance_fence.as_ref() != Some(fence)
        || proof.maintenance_executor_certified == retired
        || proof.maintenance_executor_retired_certified != retired
    {
        bail!("incorrectly fenced all-voter prefix evidence");
    }
    Ok(())
}

fn validate_observed_prefix(
    cell: &CellIdentity,
    observed: &BTreeSet<u64>,
    observation: &PrefixObservation,
) -> Result<()> {
    let proof = &observation.verification;
    let group_count =
        usize::try_from(cell.group_count).context("group inventory exceeds address space")?;
    if observation.started_ms == 0
        || observation.completed_ms < observation.started_ms
        || proof.version != 3
        || !proof.participation_certified
        || !proof.process_incarnations_certified
        || proof
            .process_incarnations
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            != *observed
        || proof.applied.keys().copied().collect::<BTreeSet<_>>() != *observed
        || proof.prefixes.len() != group_count
        || !proof.prefixes.keys().copied().eq(0..cell.group_count)
    {
        bail!("incomplete or incorrectly fenced all-voter prefix evidence");
    }
    for (group, prefix) in &proof.prefixes {
        if *group != prefix.raft_group_id || !observed.contains(&prefix.leader_id) {
            bail!("prefix identity is outside the cell");
        }
    }
    for applied in proof.applied.values() {
        if applied.len() != group_count || !applied.keys().eq(proof.prefixes.keys()) {
            bail!("missing replica group evidence");
        }
        for (group, prefix) in &proof.prefixes {
            if applied
                .get(group)
                .is_none_or(|index| *index < prefix.required_applied_index)
            {
                bail!("replica has not applied the fixed prefix");
            }
        }
    }
    Ok(())
}

fn validate_processes(plan: &[NodeInfo], observation: &PrefixObservation) -> Result<()> {
    if plan.len() != observation.verification.process_incarnations.len()
        || plan.iter().any(|node| {
            node.expected_process_incarnation.as_ref()
                != observation.verification.process_incarnations.get(&node.id)
        })
    {
        bail!("proof process incarnations differ from the saved plan");
    }
    Ok(())
}

fn fresh(observation: &PrefixObservation, now_ms: u64, acquired_ms: u64) -> Result<()> {
    fresh_timestamps(
        observation.started_ms,
        observation.completed_ms,
        now_ms,
        acquired_ms,
    )
}

fn fresh_timestamps(
    started_ms: u64,
    completed_ms: u64,
    now_ms: u64,
    acquired_ms: u64,
) -> Result<()> {
    if started_ms < acquired_ms
        || completed_ms < started_ms
        || completed_ms > now_ms
        || now_ms
            .checked_sub(started_ms)
            .is_none_or(|elapsed| elapsed > 60_000)
    {
        bail!("prefix observation is stale, predates acquisition or is in the future");
    }
    Ok(())
}

fn no_regression(before: &PrefixObservation, after: &PrefixObservation) -> Result<()> {
    for (group, prefix) in &before.verification.prefixes {
        if after
            .verification
            .prefixes
            .get(group)
            .is_none_or(|next| next.required_applied_index < prefix.required_applied_index)
        {
            bail!("fresh prefix regressed below the admitted write boundary");
        }
    }
    Ok(())
}

impl Reservation {
    /// Planned Pod replacement only. This does not admit host deletion or
    /// managed-node provider updates; those require a separate physical fence.
    pub fn progress(&self, request: ProgressRequest) -> Result<Self> {
        self.validate()?;
        let mut next = self.clone();
        let operation = next.operation.as_mut().context("no reserved operation")?;
        if operation.host.is_some() {
            bail!("planned Pod progress cannot mutate a host recovery");
        }
        let supplied_fence = match &request {
            ProgressRequest::AdmitPodDeletion { fence, .. }
            | ProgressRequest::BindPodReplacement { fence, .. }
            | ProgressRequest::CompletePodReplacement { fence, .. } => fence,
        };
        if supplied_fence != &operation.fence {
            bail!("stale executor cannot progress the reservation");
        }
        match request {
            ProgressRequest::AdmitPodDeletion {
                now_ms,
                observation,
                ..
            } => {
                if operation.replacement.is_some() {
                    bail!("original Pod is already retired; another delete is forbidden");
                }
                validate_prefix(&self.cell, &operation.fence, &observation, false)?;
                validate_processes(&operation.process_plan, &observation)?;
                fresh(&observation, now_ms, operation.acquired_ms)?;
                if let Some(previous) = &operation.admission {
                    no_regression(previous, &observation)?;
                }
                if let Some(receipt) = &self.completion {
                    no_regression(&receipt.observation, &observation)?;
                }
                if let Some(hosts) = &self.hosts {
                    no_regression(&hosts.observation, &observation)?;
                }
                operation.admission = Some(observation);
            }
            ProgressRequest::BindPodReplacement {
                pod,
                node,
                process_plan,
                ..
            } => {
                operation
                    .admission
                    .as_ref()
                    .context("source deletion was not admitted")?;
                if operation.replacement.is_some() {
                    bail!("replacement identity is already pinned; cannot refresh it");
                }
                let replacement = SourceIdentity::capture(
                    &self.cell,
                    operation.source.node_id,
                    &pod,
                    &node,
                    &process_plan,
                )?;
                if replacement.pod_uid == operation.source.pod_uid
                    || replacement.process_incarnation == operation.source.process_incarnation
                {
                    bail!("original Pod UID/process has not been retired");
                }
                if let Some(hosts) = &self.hosts {
                    let old = hosts.voter(operation.source.node_id)?;
                    let new = HostVoter::capture(&self.cell, replacement.clone(), &node)?;
                    if !old.same_host(&new) {
                        bail!("planned Pod replacement cannot change a catalogued physical host");
                    }
                }
                for (value, label) in [
                    (&replacement.pod_uid, "replacement Pod UID"),
                    (&replacement.node_uid, "replacement Node UID"),
                    (
                        &replacement.provider_instance,
                        "replacement provider identity",
                    ),
                ] {
                    identity(value, label)?;
                }
                bind_replacement(operation, replacement, process_plan)?;
            }
            ProgressRequest::CompletePodReplacement {
                now_ms,
                observation,
                ..
            } => {
                let admitted = operation.admission.as_ref().context("no admitted source")?;
                let replacement = operation
                    .replacement
                    .as_ref()
                    .context("original Pod UID remains live or unconfirmed")?;
                validate_prefix(&self.cell, &operation.fence, &observation, true)?;
                validate_processes(&operation.process_plan, &observation)?;
                fresh(&observation, now_ms, operation.acquired_ms)?;
                if observation.started_ms < admitted.completed_ms {
                    bail!("completion predates disruption admission");
                }
                no_regression(admitted, &observation)?;
                if let Some(hosts) = &mut next.hosts {
                    hosts.complete_pod_replacement(
                        replacement,
                        &operation.process_plan,
                        &observation,
                    )?;
                }
                next.completion = Some(Completion {
                    fence: operation.fence.clone(),
                    source: operation.source.clone(),
                    replacement: replacement.clone(),
                    observation,
                    host: None,
                });
                next.operation = None;
            }
        }
        next.validate()?;
        Ok(next)
    }
}

fn bind_replacement(
    operation: &mut Operation,
    replacement: SourceIdentity,
    process_plan: Vec<NodeInfo>,
) -> Result<()> {
    if operation.replacement.is_some() || process_plan.len() != operation.process_plan.len() {
        bail!("replacement is already bound or changed inventory");
    }
    for old in &operation.process_plan {
        let new = process_plan
            .iter()
            .find(|node| node.id == old.id)
            .context("replacement lost a survivor")?;
        let mut permitted = old.clone();
        if old.id == replacement.node_id {
            permitted.expected_process_incarnation = Some(replacement.process_incarnation.clone());
        }
        if serde_json::to_value(&permitted)? != serde_json::to_value(new)? {
            bail!("only the selected replacement process may be rebound");
        }
    }
    operation.process_plan = process_plan;
    operation.replacement = Some(replacement);
    Ok(())
}
