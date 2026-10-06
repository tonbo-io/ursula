//! Kubernetes startup admission before the server creates Raft transport.
//! Reuses the reservation binding CAS and local EC2 identity; never mutates provider resources.

use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use reqwest::Client;
use serde_json::Value;
use ursula_proto::admin::MaintenanceFenceState;
use ursula_proto::admin::ProcessIncarnation;
use ursula_proto::admin::StartupAdmission;

use crate::reservation::CellIdentity;
use crate::reservation::ConfigMapSnapshot;

const MAX_OBJECT_BYTES: usize = 2 * 1024 * 1024;

pub struct StartupIdentity {
    pub namespace: String,
    pub statefulset: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub node_id: u64,
    pub group_count: u32,
    pub core_count: u16,
    pub process_incarnation: ProcessIncarnation,
}

impl StartupIdentity {
    pub fn from_environment(
        node_id: u64,
        group_count: u32,
        core_count: u16,
        process_incarnation: ProcessIncarnation,
    ) -> Result<Self> {
        Ok(Self {
            namespace: std::env::var("URSULA_STARTUP_NAMESPACE")
                .context("missing startup namespace")?,
            statefulset: std::env::var("URSULA_STARTUP_STATEFULSET")
                .context("missing startup StatefulSet")?,
            pod_name: std::env::var("URSULA_STARTUP_POD_NAME")
                .context("missing startup Pod name")?,
            pod_uid: std::env::var("URSULA_STARTUP_POD_UID").context("missing startup Pod UID")?,
            node_id,
            group_count,
            core_count,
            process_incarnation,
        })
    }
}

struct LocalEc2Identity {
    instance_id: String,
}

impl LocalEc2Identity {
    fn from_nitro_file(path: &Path) -> Result<Self> {
        use std::io::Read;
        let mut value = String::new();
        std::fs::File::open(path)?
            .take(129)
            .read_to_string(&mut value)?;
        if value.len() > 128 {
            bail!("local Nitro identity exceeds size bound");
        }
        let instance_id = value.trim().to_owned();
        if !instance_id.strip_prefix("i-").is_some_and(|id| {
            !id.is_empty()
                && id.len() <= 32
                && id
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        }) {
            bail!("local Nitro instance identity is invalid");
        }
        Ok(Self { instance_id })
    }

    fn verify_node(&self, node: &Value) -> Result<()> {
        let provider = node
            .pointer("/spec/providerID")
            .and_then(Value::as_str)
            .context("Node has no provider identity")?;
        let (zone, instance) = provider
            .strip_prefix("aws:///")
            .and_then(|value| value.rsplit_once('/'))
            .context("Node is not an AWS EC2 identity")?;
        if instance != self.instance_id
            || zone.is_empty()
            || !zone.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
            || node
                .pointer("/metadata/labels/topology.kubernetes.io~1zone")
                .and_then(Value::as_str)
                != Some(zone)
        {
            bail!("Kubernetes Node does not describe this local Nitro instance");
        }
        Ok(())
    }
}

async fn bounded_body(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|size| size > u64::try_from(limit).unwrap_or(0))
    {
        bail!("startup response exceeds its size bound");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            bail!("startup response exceeds its size bound");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

struct Api {
    client: Client,
    endpoint: url::Url,
    token: String,
    local_host: LocalEc2Identity,
}

impl Api {
    fn in_cluster() -> Result<Self> {
        if std::env::var("URSULA_STARTUP_PROVIDER").as_deref() != Ok("aws-nitro") {
            bail!("startup ownership requires the explicit aws-nitro provider profile");
        }
        // This read-only hostPath is one kernel-owned sysfs file, never an API
        // label, credential, configurable instance ID or metadata endpoint.
        let local_host =
            LocalEc2Identity::from_nitro_file(Path::new("/var/run/ursula-physical-instance"))?;
        let directory = Path::new("/var/run/ursula-startup");
        let ca = reqwest::Certificate::from_pem(&std::fs::read(directory.join("ca.crt"))?)?;
        let token = std::fs::read_to_string(directory.join("token"))?;
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .https_only(true)
            .tls_built_in_root_certs(false)
            .add_root_certificate(ca)
            .build()?;
        Ok(Self {
            client,
            endpoint: "https://kubernetes.default.svc/".parse()?,
            token,
            local_host,
        })
    }

    fn url(&self, segments: &[&str]) -> Result<url::Url> {
        let mut url = self.endpoint.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|()| anyhow::anyhow!("invalid API endpoint"))?;
        path.clear();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }

    async fn object(&self, segments: &[&str], offered: Option<&Value>) -> Result<Value> {
        let url = self.url(segments)?;
        let request = if let Some(offered) = offered {
            self.client.put(url).json(offered)
        } else {
            self.client.get(url)
        };
        let response = request
            .bearer_auth(self.token.trim())
            .send()
            .await?
            .error_for_status()?;
        let bytes = bounded_body(response, MAX_OBJECT_BYTES).await?;
        serde_json::from_slice(&bytes).context("invalid startup API object")
    }

    async fn pod_node(&self, identity: &StartupIdentity) -> Result<(Value, Value)> {
        let pod = self
            .object(
                &[
                    "api",
                    "v1",
                    "namespaces",
                    &identity.namespace,
                    "pods",
                    &identity.pod_name,
                ],
                None,
            )
            .await?;
        let name = pod
            .pointer("/spec/nodeName")
            .and_then(Value::as_str)
            .context("startup Pod has no scheduled Node")?;
        let node = self.object(&["api", "v1", "nodes", name], None).await?;
        self.local_host.verify_node(&node)?;
        Ok((pod, node))
    }

    async fn admit(&self, identity: &StartupIdentity) -> Result<StartupAdmission> {
        let namespace = self
            .object(&["api", "v1", "namespaces", &identity.namespace], None)
            .await?;
        let statefulset = self
            .object(
                &[
                    "apis",
                    "apps",
                    "v1",
                    "namespaces",
                    &identity.namespace,
                    "statefulsets",
                    &identity.statefulset,
                ],
                None,
            )
            .await?;
        let cell = CellIdentity::capture(
            &namespace,
            &statefulset,
            identity.group_count,
            identity.core_count,
        )?;
        let name = format!("{}-maintenance", identity.statefulset);
        let path = [
            "api",
            "v1",
            "namespaces",
            &identity.namespace,
            "configmaps",
            &name,
        ];
        let raw = self.object(&path, None).await?;
        let before = ConfigMapSnapshot::parse(raw.clone(), &cell)?;
        let (pod, node) = self.pod_node(identity).await?;
        let proposal = before.propose_startup(
            identity.node_id,
            &identity.pod_uid,
            &pod,
            &node,
            identity.process_incarnation.clone(),
        )?;
        // Resample physical identity before committing the immutable boot. No
        // automatic retry can turn a different Pod/Node into this attempt.
        let (pod2, node2) = self.pod_node(identity).await?;
        let second = before.propose_startup(
            identity.node_id,
            &identity.pod_uid,
            &pod2,
            &node2,
            identity.process_incarnation.clone(),
        )?;
        match (&proposal, &second) {
            (Some(a), Some(b)) if a.document().get("data") == b.document().get("data") => (),
            (None, None) => (),
            _ => bail!("startup identity changed before binding"),
        }
        let acknowledged = if let Some(proposal) = proposal {
            let response = self.object(&path, Some(proposal.document())).await?;
            // A timeout/ambiguous PUT never starts the server or clears ownership.
            before.acknowledge(&proposal, response)?
        } else {
            let current = self.object(&path, None).await?;
            if current.get("data") != raw.get("data")
                || current.pointer("/metadata/uid") != raw.pointer("/metadata/uid")
            {
                bail!("reservation changed during idle startup");
            }
            ConfigMapSnapshot::parse(current, &cell)?
        };
        // Revalidate the live physical owner after the acknowledged bind as well.
        let (after_pod, after_node) = self.pod_node(identity).await?;
        if physical_identity(&pod2, &node2) != physical_identity(&after_pod, &after_node) {
            bail!("physical startup owner changed after admission");
        }
        let maintenance_fence = if let Some(operation) = acknowledged.state().operation() {
            MaintenanceFenceState::Activating {
                fence: operation.fence.clone(),
            }
        } else if let Some(completion) = acknowledged.state().completion() {
            MaintenanceFenceState::Retired {
                fence: completion.fence.clone(),
            }
        } else {
            MaintenanceFenceState::AwaitingReservation
        };
        let admission = StartupAdmission {
            process_incarnation: identity.process_incarnation.clone(),
            maintenance_fence,
        };
        admission.validate().map_err(anyhow::Error::msg)?;
        Ok(admission)
    }
}

fn physical_identity(pod: &Value, node: &Value) -> Value {
    serde_json::json!({"pod_uid":pod.pointer("/metadata/uid"),"pod_deleted":pod.pointer("/metadata/deletionTimestamp"),
        "owner":pod.pointer("/metadata/ownerReferences"),"node_name":pod.pointer("/spec/nodeName"),
        "node_uid":node.pointer("/metadata/uid"),"provider":node.pointer("/spec/providerID"),
        "node_deleted":node.pointer("/metadata/deletionTimestamp"),"zone":node.pointer("/metadata/labels/topology.kubernetes.io~1zone")})
}

/// Returns acknowledged closed authority for a fresh server incarnation.
/// The caller loads both identities before exposing transport or admin routes.
pub async fn admit_in_cluster(identity: StartupIdentity) -> Result<StartupAdmission> {
    tokio::time::timeout(Duration::from_secs(60), Api::in_cluster()?.admit(&identity))
        .await
        .context("startup admission exceeded its deadline")?
}

#[cfg(test)]
mod tests;
