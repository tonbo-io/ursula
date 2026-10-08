//! Exact-host recovery intents and irreversible receipts in the common CAS.
//! Provider observations are supplied by the reviewed platform adapter. This
//! policy performs no provider calls and cannot authenticate those observations.

use std::collections::BTreeSet;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use ursula_proto::admin::MaintenanceFence;

use super::CellIdentity;
use super::Completion;
use super::HostInventory;
use super::HostVoter;
use super::Operation;
use super::PrefixObservation;
use super::Reservation;
use super::SourceIdentity;
use super::bind_replacement;
use super::fresh;
use super::fresh_timestamps;
use super::hosts::ready;
use super::identity;
use super::inventory::selected_pod_metadata;
use super::no_regression;
use super::validate_observed_prefix;
use super::validate_prefix;
use super::validate_processes;
use crate::NodeInfo;
use crate::quorum::SurvivingQuorumVerification;

mod restage;

pub use restage::ReplacementRetirement;
pub use restage::RetiredHostReplacement;

const MAX_POD_RETIREMENTS: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SurvivingPrefixObservation {
    pub started_ms: u64,
    pub completed_ms: u64,
    pub verification: SurvivingQuorumVerification,
}

impl SurvivingPrefixObservation {
    fn prefix(&self) -> PrefixObservation {
        PrefixObservation {
            started_ms: self.started_ms,
            completed_ms: self.completed_ms,
            verification: self.verification.verification.clone(),
        }
    }

    fn validate(&self, cell: &CellIdentity, source: u64, fence: &MaintenanceFence) -> Result<()> {
        let survivors = cell
            .voter_ids
            .iter()
            .copied()
            .filter(|id| *id != source)
            .collect::<BTreeSet<_>>();
        let report = &self.verification;
        let proof = &report.verification;
        if report.excluded_voter_id != source
            || report.configured_voter_ids != cell.voter_ids
            || report.surviving_voter_ids != survivors
            || survivors.len() != 2
            || report.full_redundancy_restored
            || !proof.maintenance_executor_certified
            || proof.maintenance_executor_retired_certified
        {
            bail!(
                "host termination requires explicitly excluded source and two active certified survivors"
            );
        }
        validate_observed_prefix(cell, &survivors, &self.prefix())?;
        let admitted_fence = proof
            .maintenance_fence
            .as_ref()
            .context("missing survivor executor token")?;
        if admitted_fence.reservation_id() != fence.reservation_id()
            || admitted_fence.generation() > fence.generation()
        {
            bail!("survivor admission belongs to another operation or future executor");
        }
        Ok(())
    }
}

/// The adapter must observe this exact instance through its authenticated provider
/// API. Stopped, shutting-down, not-found and unknown are not terminal fences.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostTerminationObservation {
    pub started_ms: u64,
    pub completed_ms: u64,
    pub provider_instance: String,
    pub terminal_state: String,
}

impl HostTerminationObservation {
    fn validate(&self, source: &HostVoter, admission: &SurvivingPrefixObservation) -> Result<()> {
        self.validate_since(source, admission.completed_ms)
    }

    fn validate_since(&self, source: &HostVoter, admission_completed_ms: u64) -> Result<()> {
        if self.provider_instance != source.source.provider_instance
            || self.terminal_state != "terminated"
            || self.started_ms < admission_completed_ms
            || self.completed_ms < self.started_ms
        {
            bail!("no irreversible terminal observation of the exact admitted provider instance");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostRecovery {
    pub source_host: HostVoter,
    /// This sticky admission is the intent to terminate only source_host.
    pub admission: Option<SurvivingPrefixObservation>,
    pub termination: Option<HostTerminationObservation>,
    /// Persist before any force delete; none may subsequently bind as replacement.
    pub pod_retirement_intents: BTreeSet<String>,
    pub replacement_host: Option<HostVoter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_retirement: Option<ReplacementRetirement>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retired_replacements: Vec<RetiredHostReplacement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_replacement_prefix: Option<SurvivingPrefixObservation>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostRequest {
    ReserveHostRecovery {
        operation_id: String,
        executor_id: String,
        node_id: u64,
        process_plan: Vec<NodeInfo>,
        now_ms: u64,
    },
    AdmitHostTermination {
        fence: MaintenanceFence,
        now_ms: u64,
        observation: SurvivingPrefixObservation,
    },
    RecordHostTermination {
        fence: MaintenanceFence,
        now_ms: u64,
        observation: HostTerminationObservation,
    },
    AdmitFencedPodRetirement {
        fence: MaintenanceFence,
        /// None records the original pre-fault Pod UID without a name refresh.
        pod: Option<Value>,
        node: Option<Value>,
    },
    BindHostReplacement {
        fence: MaintenanceFence,
        pod: Value,
        node: Value,
        process_plan: Vec<NodeInfo>,
    },
    AdmitReplacementTermination {
        fence: MaintenanceFence,
        candidate: SourceIdentity,
        now_ms: u64,
        observation: SurvivingPrefixObservation,
    },
    RecordReplacementTermination {
        fence: MaintenanceFence,
        candidate: SourceIdentity,
        now_ms: u64,
        observation: HostTerminationObservation,
    },
    RestageHostReplacement {
        fence: MaintenanceFence,
        candidate: SourceIdentity,
        now_ms: u64,
    },
    CompleteHostReplacement {
        fence: MaintenanceFence,
        now_ms: u64,
        observation: PrefixObservation,
    },
}

impl<'de> Deserialize<'de> for HostRequest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Reserve {
            operation_id: String,
            executor_id: String,
            node_id: u64,
            process_plan: Vec<NodeInfo>,
            now_ms: u64,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Survivor {
            fence: MaintenanceFence,
            now_ms: u64,
            observation: SurvivingPrefixObservation,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Termination {
            fence: MaintenanceFence,
            now_ms: u64,
            observation: HostTerminationObservation,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Retirement {
            fence: MaintenanceFence,
            pod: Option<Value>,
            node: Option<Value>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Binding {
            fence: MaintenanceFence,
            pod: Value,
            node: Value,
            process_plan: Vec<NodeInfo>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Complete {
            fence: MaintenanceFence,
            now_ms: u64,
            observation: PrefixObservation,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Restage {
            fence: MaintenanceFence,
            candidate: SourceIdentity,
            now_ms: u64,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct CandidateObservation<T> {
            fence: MaintenanceFence,
            candidate: SourceIdentity,
            now_ms: u64,
            observation: T,
        }
        let mut value = Value::deserialize(deserializer)?;
        let action = value
            .as_object_mut()
            .and_then(|object| object.remove("action"))
            .and_then(|value| value.as_str().map(str::to_owned))
            .ok_or_else(|| <D::Error as serde::de::Error>::custom("missing host action"))?;
        let parse_error = <D::Error as serde::de::Error>::custom;
        match action.as_str() {
            "reserve_host_recovery" => {
                let fields: Reserve = serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::ReserveHostRecovery {
                    operation_id: fields.operation_id,
                    executor_id: fields.executor_id,
                    node_id: fields.node_id,
                    process_plan: fields.process_plan,
                    now_ms: fields.now_ms,
                })
            }
            "admit_host_termination" => {
                let fields: Survivor = serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::AdmitHostTermination {
                    fence: fields.fence,
                    now_ms: fields.now_ms,
                    observation: fields.observation,
                })
            }
            "record_host_termination" => {
                let fields: Termination = serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::RecordHostTermination {
                    fence: fields.fence,
                    now_ms: fields.now_ms,
                    observation: fields.observation,
                })
            }
            "admit_fenced_pod_retirement" => {
                let fields: Retirement = serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::AdmitFencedPodRetirement {
                    fence: fields.fence,
                    pod: fields.pod,
                    node: fields.node,
                })
            }
            "bind_host_replacement" => {
                let fields: Binding = serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::BindHostReplacement {
                    fence: fields.fence,
                    pod: fields.pod,
                    node: fields.node,
                    process_plan: fields.process_plan,
                })
            }
            "admit_replacement_termination" => {
                let fields: CandidateObservation<SurvivingPrefixObservation> =
                    serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::AdmitReplacementTermination {
                    fence: fields.fence,
                    candidate: fields.candidate,
                    now_ms: fields.now_ms,
                    observation: fields.observation,
                })
            }
            "record_replacement_termination" => {
                let fields: CandidateObservation<HostTerminationObservation> =
                    serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::RecordReplacementTermination {
                    fence: fields.fence,
                    candidate: fields.candidate,
                    now_ms: fields.now_ms,
                    observation: fields.observation,
                })
            }
            "restage_host_replacement" => {
                let fields: Restage = serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::RestageHostReplacement {
                    fence: fields.fence,
                    candidate: fields.candidate,
                    now_ms: fields.now_ms,
                })
            }
            "complete_host_replacement" => {
                let fields: Complete = serde_json::from_value(value).map_err(parse_error)?;
                Ok(Self::CompleteHostReplacement {
                    fence: fields.fence,
                    now_ms: fields.now_ms,
                    observation: fields.observation,
                })
            }
            _ => Err(<D::Error as serde::de::Error>::custom(
                "unsupported host recovery action",
            )),
        }
    }
}

fn survivor_plan(plan: &[NodeInfo], source: u64) -> Vec<NodeInfo> {
    plan.iter()
        .filter(|node| node.id != source)
        .cloned()
        .collect()
}

/// Source must be the catalogued physical owner. Initially healthy survivor
/// boots may have changed since sampling; admission certifies them fresh and
/// then they remain immutable through takeover and replacement.
fn original_host_plan(
    hosts: &HostInventory,
    source: &SourceIdentity,
    plan: &[NodeInfo],
    now_ms: u64,
) -> Result<()> {
    let mut original = plan.to_vec();
    for node in &mut original {
        if node.id != source.node_id {
            node.expected_process_incarnation
                .as_ref()
                .context("missing fixed survivor process")?;
            node.expected_process_incarnation = hosts
                .process_plan
                .iter()
                .find(|saved| saved.id == node.id)
                .and_then(|saved| saved.expected_process_incarnation.clone());
        }
    }
    hosts.validate_source(source, &original, now_ms)
}

impl HostRecovery {
    fn validate_common(&self, cell: &CellIdentity, fence: &MaintenanceFence) -> Result<()> {
        self.source_host.validate(cell)?;
        self.validate_restage_history(cell, fence)?;
        if self.pod_retirement_intents.len() > MAX_POD_RETIREMENTS {
            bail!("fenced Pod retirement history exceeds its bound");
        }
        for uid in &self.pod_retirement_intents {
            identity(uid, "fenced Pod retirement UID")?;
        }
        if let Some(admission) = &self.admission {
            admission.validate(cell, self.source_host.source.node_id, fence)?;
        }
        if let Some(termination) = &self.termination {
            termination.validate(
                &self.source_host,
                self.admission
                    .as_ref()
                    .context("termination lacks durable intent")?,
            )?;
        }
        if !self.pod_retirement_intents.is_empty() && self.termination.is_none() {
            bail!("force deletion intent lacks an irreversible provider fence");
        }
        if let Some(replacement) = &self.replacement_host {
            replacement.validate(cell)?;
            self.check_retired_host_reuse(replacement)?;
            if self.termination.is_none()
                || !self
                    .pod_retirement_intents
                    .contains(&self.source_host.source.pod_uid)
                || self
                    .pod_retirement_intents
                    .contains(&replacement.source.pod_uid)
                || replacement.source.node_id != self.source_host.source.node_id
                || replacement.source.pod_uid == self.source_host.source.pod_uid
                || replacement.source.process_incarnation
                    == self.source_host.source.process_incarnation
                || replacement.source.node_uid == self.source_host.source.node_uid
                || replacement.source.provider_instance == self.source_host.source.provider_instance
                || replacement.failure_domain != self.source_host.failure_domain
            {
                bail!(
                    "replacement was not independently hosted or remains subject to a permitted stale delete"
                );
            }
        }
        Ok(())
    }

    pub(super) fn validate_operation(
        &self,
        state: &Reservation,
        operation: &Operation,
    ) -> Result<()> {
        self.validate_common(&state.cell, &operation.fence)?;
        let hosts = state
            .hosts
            .as_ref()
            .context("host recovery lacks pre-fault inventory")?;
        if &self.source_host != hosts.voter(operation.source.node_id)?
            || self.source_host.source != operation.source
            || self.replacement_host.as_ref().map(|host| &host.source)
                != operation.replacement.as_ref()
        {
            bail!("host recovery changed its catalogued source or bound replacement");
        }
        let mut original = operation.process_plan.clone();
        original
            .iter_mut()
            .find(|node| node.id == operation.source.node_id)
            .context("missing original target")?
            .expected_process_incarnation = Some(operation.source.process_incarnation.clone());
        original_host_plan(hosts, &operation.source, &original, operation.acquired_ms)?;
        if let Some(admission) = &self.admission {
            validate_processes(
                &survivor_plan(&operation.process_plan, operation.source.node_id),
                &admission.prefix(),
            )?;
            no_regression(&hosts.observation, &admission.prefix())?;
            if let Some(receipt) = &state.completion {
                no_regression(&receipt.observation, &admission.prefix())?;
            }
        }
        self.validate_restage_operation(state, operation)?;
        if let Some(replacement) = &self.replacement_host {
            for host in &hosts.voters {
                if host.source.node_id != operation.source.node_id
                    && (host.source.node_uid == replacement.source.node_uid
                        || host.source.provider_instance == replacement.source.provider_instance
                        || host.node_name == replacement.node_name)
                {
                    bail!("replacement shares a physical host with a survivor");
                }
            }
        }
        Ok(())
    }

    pub(super) fn validate_completion(
        &self,
        cell: &CellIdentity,
        receipt: &Completion,
    ) -> Result<()> {
        self.validate_common(cell, &receipt.fence)?;
        let admission = self
            .admission
            .as_ref()
            .context("host completion lacks admission")?;
        let replacement = self
            .replacement_host
            .as_ref()
            .context("host completion lacks physical replacement")?;
        if self.source_host.source != receipt.source || replacement.source != receipt.replacement {
            bail!("host receipt changed its source or replacement");
        }
        self.validate_restage_completion(receipt)?;
        let admitted = admission.prefix();
        no_regression(&admitted, &receipt.observation)?;
        if receipt.observation.started_ms
            < self
                .termination
                .as_ref()
                .context("host completion lacks provider fence")?
                .completed_ms
        {
            bail!("host completion predates the irreversible provider fence");
        }
        for (id, boot) in &admitted.verification.process_incarnations {
            if receipt
                .observation
                .verification
                .process_incarnations
                .get(id)
                != Some(boot)
            {
                bail!("host completion changed an admitted survivor process");
            }
        }
        Ok(())
    }

    pub fn stage(&self) -> &'static str {
        if let Some(retirement) = &self.replacement_retirement {
            if retirement.termination.is_some() {
                "host-replacement-terminated"
            } else {
                "host-replacement-termination-admitted"
            }
        } else if self.replacement_host.is_some() {
            "host-replacement-bound"
        } else if self.termination.is_some() {
            "host-terminated"
        } else if self.admission.is_some() {
            "host-termination-admitted"
        } else {
            "host-reserved"
        }
    }
}

impl Reservation {
    pub fn recover_host(&self, request: HostRequest) -> Result<Self> {
        self.validate()?;
        if let HostRequest::ReserveHostRecovery {
            operation_id,
            executor_id,
            node_id,
            mut process_plan,
            now_ms,
        } = request
        {
            if self.operation.is_some() {
                bail!("another voter remains reserved");
            }
            let hosts = self
                .hosts
                .as_ref()
                .context("host recovery requires settled pre-fault inventory")?;
            let source_host = hosts.voter(node_id)?.clone();
            original_host_plan(hosts, &source_host.source, &process_plan, now_ms)?;
            let mut next = self.clone();
            next.generation = self
                .generation
                .checked_add(1)
                .context("maintenance generation exhausted")?;
            let fence = MaintenanceFence::new(operation_id, executor_id, next.generation)
                .map_err(anyhow::Error::msg)?;
            for node in &mut process_plan {
                node.expected_maintenance_fence = Some(fence.clone());
            }
            next.operation = Some(Operation {
                fence,
                source: source_host.source.clone(),
                process_plan,
                acquired_ms: now_ms,
                admission: None,
                replacement: None,
                host: Some(HostRecovery {
                    source_host,
                    admission: None,
                    termination: None,
                    pod_retirement_intents: BTreeSet::new(),
                    replacement_host: None,
                    replacement_retirement: None,
                    retired_replacements: Vec::new(),
                    retained_replacement_prefix: None,
                }),
            });
            next.validate()?;
            return Ok(next);
        }
        let mut next = self.clone();
        let operation = next
            .operation
            .as_mut()
            .context("no reserved host recovery")?;
        let host = operation
            .host
            .as_mut()
            .context("planned Pod ownership cannot admit physical host actions")?;
        let fence = match &request {
            HostRequest::AdmitHostTermination { fence, .. }
            | HostRequest::RecordHostTermination { fence, .. }
            | HostRequest::AdmitFencedPodRetirement { fence, .. }
            | HostRequest::BindHostReplacement { fence, .. }
            | HostRequest::CompleteHostReplacement { fence, .. }
            | HostRequest::AdmitReplacementTermination { fence, .. }
            | HostRequest::RecordReplacementTermination { fence, .. }
            | HostRequest::RestageHostReplacement { fence, .. } => fence,
            HostRequest::ReserveHostRecovery { .. } => bail!("unexpected ownership request"),
        };
        if fence != &operation.fence {
            bail!("stale executor cannot progress host recovery");
        }
        match request {
            HostRequest::AdmitHostTermination {
                now_ms,
                observation,
                ..
            } => {
                if host.termination.is_some() {
                    bail!("terminal host fence is already recorded");
                }
                observation.validate(&self.cell, operation.source.node_id, &operation.fence)?;
                if observation
                    .verification
                    .verification
                    .maintenance_fence
                    .as_ref()
                    != Some(&operation.fence)
                {
                    bail!("termination requires current active survivor executor");
                }
                let prefix = observation.prefix();
                validate_processes(
                    &survivor_plan(&operation.process_plan, operation.source.node_id),
                    &prefix,
                )?;
                fresh(&prefix, now_ms, operation.acquired_ms)?;
                no_regression(
                    &self
                        .hosts
                        .as_ref()
                        .context("missing host inventory")?
                        .observation,
                    &prefix,
                )?;
                if let Some(previous) = &host.admission {
                    no_regression(&previous.prefix(), &prefix)?;
                }
                if let Some(previous) = &self.completion {
                    no_regression(&previous.observation, &prefix)?;
                }
                host.admission = Some(observation);
            }
            HostRequest::RecordHostTermination {
                now_ms,
                observation,
                ..
            } => {
                observation.validate(
                    &host.source_host,
                    host.admission
                        .as_ref()
                        .context("no persistent termination intent")?,
                )?;
                fresh_timestamps(
                    observation.started_ms,
                    observation.completed_ms,
                    now_ms,
                    operation.acquired_ms,
                )?;
                if host.termination.is_some() {
                    bail!("irreversible terminal receipt is already pinned");
                }
                host.termination = Some(observation);
            }
            HostRequest::AdmitFencedPodRetirement { pod, node, .. } => {
                host.termination
                    .as_ref()
                    .context("exact provider instance is not irreversibly fenced")?;
                if operation.replacement.is_some() {
                    bail!("no further retirement intents after replacement binding");
                }
                let uid = if let Some(pod) = pod {
                    let metadata =
                        selected_pod_metadata(&self.cell, operation.source.node_id, &pod, true)?;
                    let uid = metadata
                        .get("uid")
                        .and_then(Value::as_str)
                        .context("missing fenced Pod UID")?
                        .to_owned();
                    let node = node
                        .as_ref()
                        .context("observed Pod retirement requires the full Node observation")?;
                    let captured = SourceIdentity::capture_metadata(
                        &self.cell,
                        operation.source.node_id,
                        &pod,
                        node,
                        &operation.process_plan,
                        true,
                    )?;
                    let physical = std::iter::once(&host.source_host).chain(
                        host.retired_replacements
                            .iter()
                            .map(|retired| &retired.host),
                    );
                    if !physical.into_iter().any(|fenced| {
                        captured.node_uid == fenced.source.node_uid
                            && captured.provider_instance == fenced.source.provider_instance
                            && pod.pointer("/spec/nodeName").and_then(Value::as_str)
                                == Some(fenced.node_name.as_str())
                    }) {
                        bail!("Pod is not owned by an irreversibly fenced instance");
                    }
                    uid
                } else {
                    if node.is_some() {
                        bail!("original UID intent requires no refreshed Node object");
                    }
                    operation.source.pod_uid.clone()
                };
                identity(&uid, "fenced Pod retirement UID")?;
                host.pod_retirement_intents.insert(uid);
            }
            HostRequest::BindHostReplacement {
                pod,
                node,
                process_plan,
                ..
            } => {
                host.termination
                    .as_ref()
                    .context("old physical owner is not irreversibly fenced")?;
                if !host
                    .pod_retirement_intents
                    .contains(&operation.source.pod_uid)
                {
                    bail!("original Pod retirement intent was not persisted");
                }
                ready(&node, "replacement Node")?;
                let source = SourceIdentity::capture(
                    &self.cell,
                    operation.source.node_id,
                    &pod,
                    &node,
                    &process_plan,
                )?;
                let replacement = HostVoter::capture(&self.cell, source.clone(), &node)?;
                host.replacement_host = Some(replacement);
                bind_replacement(operation, source, process_plan)?;
            }
            HostRequest::AdmitReplacementTermination {
                candidate,
                now_ms,
                observation,
                ..
            } => {
                host.require_candidate(&candidate)?;
                host.admit_replacement_termination(
                    &self.cell,
                    &operation.fence,
                    &operation.process_plan,
                    operation.acquired_ms,
                    now_ms,
                    observation,
                )?;
            }
            HostRequest::RecordReplacementTermination {
                candidate,
                now_ms,
                observation,
                ..
            } => {
                host.require_candidate(&candidate)?;
                host.record_replacement_termination(operation.acquired_ms, now_ms, observation)?;
            }
            HostRequest::RestageHostReplacement {
                candidate, now_ms, ..
            } => {
                host.require_candidate(&candidate)?;
                host.restage_replacement(now_ms)?;
                operation.replacement = None;
                operation
                    .process_plan
                    .iter_mut()
                    .find(|node| node.id == operation.source.node_id)
                    .context("missing selected target")?
                    .expected_process_incarnation =
                    Some(operation.source.process_incarnation.clone());
            }
            HostRequest::CompleteHostReplacement {
                now_ms,
                observation,
                ..
            } => {
                let admitted = host.admission.as_ref().context("no admitted host")?;
                let replacement_host = host
                    .replacement_host
                    .as_ref()
                    .context("no bound host replacement")?;
                let replacement = operation
                    .replacement
                    .as_ref()
                    .context("no bound replacement process")?;
                validate_prefix(&self.cell, &operation.fence, &observation, true)?;
                validate_processes(&operation.process_plan, &observation)?;
                fresh(&observation, now_ms, operation.acquired_ms)?;
                no_regression(&admitted.prefix(), &observation)?;
                next.hosts
                    .as_mut()
                    .context("missing pre-fault inventory")?
                    .complete_host_replacement(
                        replacement_host.clone(),
                        &operation.process_plan,
                        &observation,
                    )?;
                next.completion = Some(Completion {
                    fence: operation.fence.clone(),
                    source: operation.source.clone(),
                    replacement: replacement.clone(),
                    observation,
                    host: Some(host.clone()),
                });
                next.operation = None;
            }
            HostRequest::ReserveHostRecovery { .. } => bail!("unexpected ownership request"),
        }
        next.validate()?;
        Ok(next)
    }
}
