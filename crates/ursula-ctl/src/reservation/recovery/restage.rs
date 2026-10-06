//! Resume one lost voter slot after fencing a failed, already-bound candidate.
//! Retain physical receipts, stale-delete UIDs and the newest survivor prefix.

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::MaintenanceFence;

use super::HostRecovery;
use super::HostTerminationObservation;
use super::MAX_POD_RETIREMENTS;
use super::SurvivingPrefixObservation;
use super::survivor_plan;
use crate::NodeInfo;
use crate::reservation::CellIdentity;
use crate::reservation::Completion;
use crate::reservation::HostInventory;
use crate::reservation::HostVoter;
use crate::reservation::Operation;
use crate::reservation::Reservation;
use crate::reservation::SourceIdentity;
use crate::reservation::fresh;
use crate::reservation::fresh_timestamps;
use crate::reservation::no_regression;
use crate::reservation::validate_processes;

const MAX_RETIRED_REPLACEMENTS: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementRetirement {
    pub candidate: SourceIdentity,
    pub admission: SurvivingPrefixObservation,
    pub termination: Option<HostTerminationObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredHostReplacement {
    pub host: HostVoter,
    pub admission_completed_ms: u64,
    pub termination: HostTerminationObservation,
}

impl HostRecovery {
    pub(super) fn require_candidate(&self, candidate: &SourceIdentity) -> Result<()> {
        if self.replacement_host.as_ref().map(|host| &host.source) != Some(candidate) {
            bail!("candidate identity differs from the current bound replacement");
        }
        Ok(())
    }

    pub(in crate::reservation) fn uses_restage_schema(&self) -> bool {
        self.replacement_retirement.is_some()
            || !self.retired_replacements.is_empty()
            || self.retained_replacement_prefix.is_some()
    }

    pub(super) fn check_retired_host_reuse(&self, candidate: &HostVoter) -> Result<()> {
        if self.retired_replacements.iter().any(|old| {
            old.host.source.pod_uid == candidate.source.pod_uid
                || old.host.source.node_uid == candidate.source.node_uid
                || old.host.source.provider_instance == candidate.source.provider_instance
                || old.host.source.process_incarnation == candidate.source.process_incarnation
        }) {
            bail!("a fenced replacement identity cannot be rebound");
        }
        Ok(())
    }

    pub(super) fn validate_restage_history(
        &self,
        cell: &CellIdentity,
        fence: &MaintenanceFence,
    ) -> Result<()> {
        if self.retired_replacements.len() > MAX_RETIRED_REPLACEMENTS
            || self.retired_replacements.is_empty() != self.retained_replacement_prefix.is_none()
        {
            bail!("replacement history exceeds its bound or lacks its retained prefix");
        }
        let mut previous_end = self
            .termination
            .as_ref()
            .map_or(0, |value| value.completed_ms);
        for (position, old) in self.retired_replacements.iter().enumerate() {
            old.host.validate(cell)?;
            old.termination
                .validate_since(&old.host, old.admission_completed_ms)?;
            if old.admission_completed_ms < previous_end
                || old.host.source.node_id != self.source_host.source.node_id
                || old.host.failure_domain != self.source_host.failure_domain
                || old.host.source.pod_uid == self.source_host.source.pod_uid
                || old.host.source.node_uid == self.source_host.source.node_uid
                || old.host.source.provider_instance == self.source_host.source.provider_instance
                || old.host.source.process_incarnation
                    == self.source_host.source.process_incarnation
                || !self
                    .pod_retirement_intents
                    .contains(&old.host.source.pod_uid)
                || self
                    .retired_replacements
                    .iter()
                    .take(position)
                    .any(|previous| {
                        previous.host.source.pod_uid == old.host.source.pod_uid
                            || previous.host.source.node_uid == old.host.source.node_uid
                            || previous.host.source.provider_instance
                                == old.host.source.provider_instance
                            || previous.host.source.process_incarnation
                                == old.host.source.process_incarnation
                    })
            {
                bail!("fenced replacement history changed ownership, chronology or UID tombstones");
            }
            previous_end = old.termination.completed_ms;
        }
        if let Some(prefix) = &self.retained_replacement_prefix {
            prefix.validate(cell, self.source_host.source.node_id, fence)?;
            let last = self
                .retired_replacements
                .last()
                .context("no retired replacement")?;
            if prefix.completed_ms != last.admission_completed_ms {
                bail!("retained prefix is not the last fenced candidate's admission");
            }
        }
        if let Some(retirement) = &self.replacement_retirement {
            let candidate = self
                .replacement_host
                .as_ref()
                .context("retirement lacks a bound candidate")?;
            self.require_candidate(&retirement.candidate)?;
            retirement
                .admission
                .validate(cell, self.source_host.source.node_id, fence)?;
            if retirement.admission.started_ms < previous_end {
                bail!("candidate retirement predates the preceding physical fence");
            }
            if let Some(receipt) = &retirement.termination {
                receipt.validate(candidate, &retirement.admission)?;
            }
        }
        Ok(())
    }

    pub(in crate::reservation) fn validate_retired_host_placement(
        &self,
        hosts: &HostInventory,
    ) -> Result<()> {
        for old in &self.retired_replacements {
            if hosts.voters.iter().any(|survivor| {
                survivor.source.node_id != self.source_host.source.node_id
                    && (survivor.source.node_uid == old.host.source.node_uid
                        || survivor.source.provider_instance == old.host.source.provider_instance
                        || survivor.node_name == old.host.node_name)
            }) {
                bail!("a retired replacement shares a physical host with a survivor");
            }
        }
        Ok(())
    }

    pub(super) fn validate_restage_operation(
        &self,
        state: &Reservation,
        operation: &Operation,
    ) -> Result<()> {
        self.validate_retired_host_placement(state.hosts.as_ref().context("no pre-fault hosts")?)?;
        for proof in self.retained_replacement_prefix.iter().chain(
            self.replacement_retirement
                .iter()
                .map(|value| &value.admission),
        ) {
            validate_processes(
                &survivor_plan(&operation.process_plan, operation.source.node_id),
                &proof.prefix(),
            )?;
            no_regression(
                &self
                    .admission
                    .as_ref()
                    .context("no original host admission")?
                    .prefix(),
                &proof.prefix(),
            )?;
            if let Some(floor) = &self.retained_replacement_prefix {
                no_regression(&floor.prefix(), &proof.prefix())?;
            }
        }
        Ok(())
    }

    pub(super) fn validate_restage_completion(&self, receipt: &Completion) -> Result<()> {
        if self.replacement_retirement.is_some() {
            bail!(
                "an admitted asynchronous candidate termination cannot be released into completion"
            );
        }
        if let Some(floor) = &self.retained_replacement_prefix {
            no_regression(&floor.prefix(), &receipt.observation)?;
            for (id, boot) in &floor.verification.verification.process_incarnations {
                if receipt
                    .observation
                    .verification
                    .process_incarnations
                    .get(id)
                    != Some(boot)
                {
                    bail!("completion changed a survivor of candidate retirement");
                }
            }
            if receipt.observation.started_ms
                < self
                    .retired_replacements
                    .last()
                    .context("no retired candidate")?
                    .termination
                    .completed_ms
            {
                bail!("completion predates the last candidate's physical fence");
            }
        }
        Ok(())
    }

    pub(super) fn admit_replacement_termination(
        &mut self,
        cell: &CellIdentity,
        fence: &MaintenanceFence,
        plan: &[NodeInfo],
        acquired_ms: u64,
        now_ms: u64,
        observation: SurvivingPrefixObservation,
    ) -> Result<()> {
        let candidate = self
            .replacement_host
            .as_ref()
            .context("no bound candidate to fence")?;
        if self.retired_replacements.len() >= MAX_RETIRED_REPLACEMENTS
            || self.pod_retirement_intents.len() >= MAX_POD_RETIREMENTS
            || self
                .replacement_retirement
                .as_ref()
                .is_some_and(|value| value.termination.is_some())
        {
            bail!("candidate fence is already terminal or its durable history budget is exhausted");
        }
        observation.validate(cell, candidate.source.node_id, fence)?;
        if observation
            .verification
            .verification
            .maintenance_fence
            .as_ref()
            != Some(fence)
        {
            bail!("candidate termination requires the current active survivor executor");
        }
        let proof = observation.prefix();
        fresh(&proof, now_ms, acquired_ms)?;
        validate_processes(&survivor_plan(plan, candidate.source.node_id), &proof)?;
        no_regression(
            &self
                .admission
                .as_ref()
                .context("no original host admission")?
                .prefix(),
            &proof,
        )?;
        if let Some(floor) = &self.retained_replacement_prefix {
            no_regression(&floor.prefix(), &proof)?;
        }
        if let Some(previous) = &self.replacement_retirement {
            no_regression(&previous.admission.prefix(), &proof)?;
        }
        self.replacement_retirement = Some(ReplacementRetirement {
            candidate: candidate.source.clone(),
            admission: observation,
            termination: None,
        });
        Ok(())
    }

    pub(super) fn record_replacement_termination(
        &mut self,
        acquired_ms: u64,
        now_ms: u64,
        observation: HostTerminationObservation,
    ) -> Result<()> {
        let candidate = self
            .replacement_host
            .as_ref()
            .context("no bound candidate")?;
        let intent = self
            .replacement_retirement
            .as_mut()
            .context("no durable candidate termination intent")?;
        if intent.termination.is_some() {
            bail!("candidate terminal receipt is already pinned");
        }
        observation.validate(candidate, &intent.admission)?;
        fresh_timestamps(
            observation.started_ms,
            observation.completed_ms,
            now_ms,
            acquired_ms,
        )?;
        intent.termination = Some(observation);
        Ok(())
    }

    pub(super) fn restage_replacement(&mut self, now_ms: u64) -> Result<()> {
        let intent = self
            .replacement_retirement
            .as_ref()
            .context("no candidate termination intent")?;
        let termination = intent
            .termination
            .as_ref()
            .context("bound candidate is not irreversibly fenced")?;
        let host = self
            .replacement_host
            .as_ref()
            .context("no bound candidate")?;
        if now_ms < termination.completed_ms
            || self.retired_replacements.len() >= MAX_RETIRED_REPLACEMENTS
            || self.pod_retirement_intents.len() >= MAX_POD_RETIREMENTS
        {
            bail!("restaging predates the terminal receipt or exhausts its durable history bound");
        }
        // This CAS both clears the binding and fences every delayed UID delete.
        // The adapter may delete only after acknowledging this entire proposal.
        self.pod_retirement_intents
            .insert(host.source.pod_uid.clone());
        self.retired_replacements.push(RetiredHostReplacement {
            host: host.clone(),
            admission_completed_ms: intent.admission.completed_ms,
            termination: termination.clone(),
        });
        self.retained_replacement_prefix = Some(intent.admission.clone());
        self.replacement_host = None;
        self.replacement_retirement = None;
        Ok(())
    }
}
