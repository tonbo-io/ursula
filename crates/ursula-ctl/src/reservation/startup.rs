//! Startup claims use the existing physical/process binding CAS, before Raft exists.
use anyhow::Result;
use anyhow::bail;
use serde_json::Value;
use ursula_proto::admin::ProcessIncarnation;

use super::CasProposal;
use super::ConfigMapSnapshot;
use super::HostRequest;
use super::HostVoter;
use super::ProgressRequest;
use super::ReservationRequest;
use super::SourceIdentity;

impl ConfigMapSnapshot {
    /// No proposal means the catalogued idle physical owner may restart. A
    /// replacement must commit the returned binding before starting transport.
    /// A bound boot cannot be refreshed by restarting the same container.
    pub fn propose_startup(
        &self,
        node_id: u64,
        pod_uid: &str,
        pod: &Value,
        node: &Value,
        boot: ProcessIncarnation,
    ) -> Result<Option<CasProposal>> {
        let state = self.state();
        state.validate()?;
        let hosts = state
            .hosts()
            .ok_or_else(|| anyhow::anyhow!("startup requires settled pre-fault inventory"))?;
        let mut plan = state
            .operation()
            .map_or(&hosts.process_plan, |operation| &operation.process_plan)
            .clone();
        let target = plan
            .iter_mut()
            .find(|target| target.id == node_id)
            .ok_or_else(|| anyhow::anyhow!("unknown startup voter"))?;
        target.expected_process_incarnation = Some(boot);
        let source = SourceIdentity::capture(state.cell(), node_id, pod, node, &plan)?;
        if source.pod_uid != pod_uid {
            bail!("startup Pod UID changed from its Downward API identity");
        }
        let observed = HostVoter::capture(state.cell(), source.clone(), node)?;
        if let Some(operation) = state.operation() {
            if operation.source.node_id != node_id {
                bail!("a fixed survivor cannot refresh its boot during maintenance");
            }
            if source.pod_uid == operation.source.pod_uid {
                bail!("retired original Pod cannot restart during replacement");
            }
            if operation.replacement.is_some() {
                bail!("replacement boot is already bound; restarting cannot refresh it");
            }
            let request = if operation.host.is_some() {
                ReservationRequest::Host(HostRequest::BindHostReplacement {
                    fence: operation.fence.clone(),
                    pod: pod.clone(),
                    node: node.clone(),
                    process_plan: plan,
                })
            } else {
                ReservationRequest::Progress(ProgressRequest::BindPodReplacement {
                    fence: operation.fence.clone(),
                    pod: pod.clone(),
                    node: node.clone(),
                    process_plan: plan,
                })
            };
            return self.transition(request).map(Some);
        }
        let catalogued = hosts.voter(node_id)?;
        if source.pod_uid != catalogued.source.pod_uid || !observed.same_host(catalogued) {
            bail!("idle startup is not the catalogued physical Pod owner");
        }
        Ok(None)
    }
}
