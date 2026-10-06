//! Capture immutable cell/source identities from complete Kubernetes objects.
//! These are observations, not host-fencing receipts or disruption authority.

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde_json::Value;

use super::CellIdentity;
use super::Reservation;
use super::SourceIdentity;
use crate::NodeInfo;

fn live_metadata<'a>(object: &'a Value, kind: &str) -> Result<&'a Value> {
    if object.get("kind").and_then(Value::as_str) != Some(kind) {
        bail!("expected complete {kind} object");
    }
    let metadata = object.get("metadata").context("missing object metadata")?;
    if metadata
        .get("deletionTimestamp")
        .is_some_and(|value| !value.is_null())
    {
        bail!("{kind} is already deleting");
    }
    Ok(metadata)
}

fn field(object: &Value, key: &str) -> Result<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("missing immutable metadata {key}"))
}

impl CellIdentity {
    pub fn capture(
        namespace: &Value,
        statefulset: &Value,
        group_count: u32,
        core_count: u16,
    ) -> Result<Self> {
        if statefulset
            .pointer("/spec/replicas")
            .and_then(Value::as_u64)
            != Some(3)
        {
            bail!("maintenance inventory requires exactly three StatefulSet replicas");
        }
        let namespace = live_metadata(namespace, "Namespace")?;
        let statefulset = live_metadata(statefulset, "StatefulSet")?;
        let name = field(namespace, "name")?;
        if statefulset.get("namespace").and_then(Value::as_str) != Some(name.as_str()) {
            bail!("StatefulSet belongs to another namespace");
        }
        let cell = Self {
            namespace: name,
            namespace_uid: field(namespace, "uid")?,
            statefulset: field(statefulset, "name")?,
            statefulset_uid: field(statefulset, "uid")?,
            group_count,
            core_count,
            voter_ids: [1, 2, 3].into_iter().collect(),
        };
        // Validate the same cell contract as reservation parsing/bootstrap.
        Reservation::initial(cell.clone())?;
        Ok(cell)
    }
}

impl SourceIdentity {
    pub fn capture(
        cell: &CellIdentity,
        node_id: u64,
        pod: &Value,
        node: &Value,
        process_plan: &[NodeInfo],
    ) -> Result<Self> {
        Reservation::initial(cell.clone())?;
        let metadata = live_metadata(pod, "Pod")?;
        let node_metadata = live_metadata(node, "Node")?;
        let ordinal = node_id.checked_sub(1).context("invalid source voter")?;
        let expected_name = format!("{}-{ordinal}", cell.statefulset);
        let owned = metadata
            .get("ownerReferences")
            .and_then(Value::as_array)
            .is_some_and(|owners| {
                owners.iter().any(|owner| {
                    owner.get("uid").and_then(Value::as_str) == Some(cell.statefulset_uid.as_str())
                        && owner.get("kind").and_then(Value::as_str) == Some("StatefulSet")
                        && owner.get("controller").and_then(Value::as_bool) == Some(true)
                })
            });
        if !owned
            || metadata.get("namespace").and_then(Value::as_str) != Some(cell.namespace.as_str())
            || metadata.get("name").and_then(Value::as_str) != Some(expected_name.as_str())
            || pod
                .pointer("/spec/nodeName")
                .and_then(Value::as_str)
                .is_none()
            || pod.pointer("/spec/nodeName") != node_metadata.get("name")
        {
            bail!("Pod/Node identity does not belong to the selected voter and cell");
        }
        let target = process_plan
            .iter()
            .find(|node| node.id == node_id)
            .context("missing selected process")?;
        let source = Self {
            node_id,
            pod_name: expected_name,
            pod_uid: field(metadata, "uid")?,
            node_uid: field(node_metadata, "uid")?,
            provider_instance: node
                .pointer("/spec/providerID")
                .and_then(Value::as_str)
                .context("missing observed provider identity")?
                .to_owned(),
            process_incarnation: target
                .expected_process_incarnation
                .clone()
                .context("missing selected process pin")?,
        };
        source.validate(cell)?;
        Ok(source)
    }
}
