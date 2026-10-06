//! Durable managed-operation client. A stopped CLI never owns executor progress.

use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use serde::de::DeserializeOwned;
use ursula_control::ControlProjection;
use ursula_control::ControlResponse;
use ursula_control::GroupMigration;
use ursula_control::MigrationOperationRequest;
use ursula_control::MigrationPhase;

use crate::NodeInfo;

pub struct OperationClient {
    client: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct RefusedRequest(String);

impl OperationClient {
    pub fn new(timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            bail!("HTTP timeout must be nonzero");
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(timeout)
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    async fn request<T: DeserializeOwned>(
        &self,
        nodes: &[NodeInfo],
        path: &str,
        body: Option<&MigrationOperationRequest>,
    ) -> Result<T> {
        let mut last = anyhow!("no reachable managed admin endpoint");
        for node in nodes {
            let url = node.admin_url.join(path)?;
            let request = if let Some(body) = body {
                self.client.post(url).json(body)
            } else {
                self.client.get(url)
            };
            let mut response = match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    last = error.into();
                    continue;
                }
            };
            let status = response.status();
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if bytes.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 {
                    bail!("managed operation response exceeds 16 MiB");
                }
                bytes.extend_from_slice(&chunk);
            }
            if status.is_success() {
                return serde_json::from_slice(&bytes).context("decode managed operation response");
            }
            let message = format!(
                "node {}: HTTP {status}: {}",
                node.id,
                String::from_utf8_lossy(&bytes)
            );
            if status.is_client_error() {
                return Err(RefusedRequest(message).into());
            }
            last = anyhow!(message);
        }
        Err(last)
    }

    pub async fn submit(
        &self,
        nodes: &[NodeInfo],
        request: &MigrationOperationRequest,
    ) -> Result<u64> {
        match self
            .request(nodes, "/__ursula/control/operations", Some(request))
            .await?
        {
            ControlResponse::MigrationStarted { migration_id } => Ok(migration_id),
            ControlResponse::Rejected { reason } => bail!("{reason}"),
            _ => bail!("unexpected operation submission response"),
        }
    }

    pub async fn list(&self, nodes: &[NodeInfo]) -> Result<ControlProjection> {
        let view: ControlProjection = self
            .request(nodes, "/__ursula/control/operations", None)
            .await?;
        view.validate().map_err(anyhow::Error::msg)?;
        Ok(view)
    }

    pub async fn status(&self, nodes: &[NodeInfo], id: u64) -> Result<GroupMigration> {
        let operation: GroupMigration = self
            .request(nodes, &format!("/__ursula/control/operations/{id}"), None)
            .await?;
        if operation.migration_id != id || operation.managed.is_none() {
            bail!("operation identity differs or lacks managed intent");
        }
        Ok(operation)
    }

    /// Resume observing the same durable ID. Timeout leaves server work active.
    pub async fn wait(
        &self,
        nodes: &[NodeInfo],
        id: u64,
        timeout: Duration,
        interval: Duration,
    ) -> Result<GroupMigration> {
        if timeout.is_zero() || interval.is_zero() {
            bail!("wait timeout and poll interval must be nonzero");
        }
        tokio::time::timeout(timeout, async {
            loop {
                match self.status(nodes, id).await {
                    Ok(operation) if operation.phase == MigrationPhase::Succeeded => return Ok(operation),
                    Ok(operation) if operation.phase == MigrationPhase::Failed => bail!("operation {id} failed: {:?}", operation.last_error),
                    Ok(_) => {},
                    Err(error) if error.is::<RefusedRequest>() => return Err(error),
                    Err(error) => tracing::warn!(operation_id = id, %error, "operation status temporarily unavailable"),
                }
                tokio::time::sleep(interval).await;
            }
        }).await.map_err(|_| anyhow!("timed out observing operation {id}; its durable server execution remains active"))?
    }
}
