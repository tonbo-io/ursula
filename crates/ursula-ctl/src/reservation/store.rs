//! Whole-object ConfigMap CAS proposals and exact API acknowledgements.
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde_json::Value;

use super::CellIdentity;
use super::OwnershipRequest;
use super::ProgressRequest;
use super::Reservation;
use super::ReservationRequest;
use super::identity;

const STATE_KEY: &str = "reservation";

/// One complete ConfigMap response, not independently sampled data fields.
#[derive(Debug, Clone)]
pub struct ConfigMapSnapshot {
    document: Value,
    state: Reservation,
}

/// An immutable proposal derived from a specific complete store snapshot.
#[derive(Debug, Clone)]
pub struct CasProposal {
    document: Value,
    previous_state: Value,
}

impl CasProposal {
    pub fn document(&self) -> &Value {
        &self.document
    }
}

impl ConfigMapSnapshot {
    pub fn state(&self) -> &Reservation {
        &self.state
    }

    pub fn parse(document: Value, expected_cell: &CellIdentity) -> Result<Self> {
        let data = document
            .get("data")
            .and_then(Value::as_object)
            .context("missing ConfigMap string data")?;
        if data.values().any(|value| !value.is_string()) {
            bail!("ConfigMap data must retain string values");
        }
        let state_text = document
            .get("data")
            .and_then(|data| data.get(STATE_KEY))
            .and_then(Value::as_str)
            .context("missing persistent reservation; do not initialize on recovery")?;
        let state: Reservation =
            serde_json::from_str(state_text).context("parse complete reservation")?;
        state.validate()?;
        if &state.cell != expected_cell
            || document.get("apiVersion").and_then(Value::as_str) != Some("v1")
            || document.get("kind").and_then(Value::as_str) != Some("ConfigMap")
        {
            bail!("reservation store belongs to another cell or unsupported kind");
        }
        let metadata = document
            .get("metadata")
            .context("missing ConfigMap metadata")?;
        let expected_name = format!("{}-maintenance", expected_cell.statefulset);
        if metadata.get("namespace").and_then(Value::as_str)
            != Some(expected_cell.namespace.as_str())
            || metadata.get("name").and_then(Value::as_str) != Some(expected_name.as_str())
            || metadata
                .get("deletionTimestamp")
                .is_some_and(|value| !value.is_null())
            || metadata
                .get("ownerReferences")
                .is_some_and(|owners| owners.as_array().is_none_or(|items| !items.is_empty()))
        {
            bail!("reservation store is mismatched, deleting or owned by an ephemeral object");
        }
        for field in ["uid", "resourceVersion"] {
            identity(
                metadata
                    .get(field)
                    .and_then(Value::as_str)
                    .context("missing ConfigMap CAS precondition")?,
                field,
            )?;
        }
        if let Some(annotations) = metadata.get("annotations") {
            annotations
                .as_object()
                .context("malformed ConfigMap annotations")?;
            for hook in ["helm.sh/hook", "argocd.argoproj.io/hook"] {
                if annotations.get(hook).is_some() {
                    bail!("persistent reservation must not be a disposable hook");
                }
            }
        }
        Ok(Self { document, state })
    }

    pub fn propose(&self, request: OwnershipRequest) -> Result<CasProposal> {
        self.propose_state(self.state.propose(request)?)
    }

    pub fn transition(&self, request: ReservationRequest) -> Result<CasProposal> {
        match request {
            ReservationRequest::Ownership(request) => self.propose(request),
            ReservationRequest::Progress(request) => self.progress(request),
            ReservationRequest::Inventory(request) => {
                self.propose_state(self.state.publish_hosts(request)?)
            }
        }
    }

    pub fn progress(&self, request: ProgressRequest) -> Result<CasProposal> {
        self.propose_state(self.state.progress(request)?)
    }

    fn propose_state(&self, next: Reservation) -> Result<CasProposal> {
        let mut document = self.document.clone();
        document
            .get_mut("data")
            .and_then(Value::as_object_mut)
            .context("invalid ConfigMap data")?
            .insert(
                STATE_KEY.to_owned(),
                Value::String(serde_json::to_string(&next)?),
            );
        // Retain UID and resourceVersion exactly. The platform submits replace,
        // never apply, and a conflict stops without refreshing this authority.
        Ok(CasProposal {
            document,
            previous_state: serde_json::to_value(&self.state)?,
        })
    }

    pub fn acknowledge(&self, proposed: &CasProposal, response: Value) -> Result<Self> {
        let result = Self::parse(response, &self.state.cell)?;
        let before = self
            .document
            .get("metadata")
            .context("missing prior metadata")?;
        let after = result
            .document
            .get("metadata")
            .context("missing response metadata")?;
        let proposed_metadata = proposed
            .document
            .get("metadata")
            .context("missing proposal metadata")?;
        if proposed.previous_state != serde_json::to_value(&self.state)?
            || proposed_metadata.get("uid") != before.get("uid")
            || proposed_metadata.get("resourceVersion") != before.get("resourceVersion")
            || before.get("uid") != after.get("uid")
            || before.get("resourceVersion") == after.get("resourceVersion")
            || result.document.get("data") != proposed.document.get("data")
        {
            bail!("CAS was not acknowledged for this exact persistent store and proposed state");
        }
        Ok(result)
    }
}
