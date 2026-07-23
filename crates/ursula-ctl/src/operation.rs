//! Operation providers: how ursulactl reaches each node's loopback-bound admin
//! plane and restarts it. Nodes carry no cluster-mutation surface on the
//! network, so every operator request is tunnelled and every restart runs
//! through the same transport.
//!
//! - `direct`  — admin URL already reachable; observe-only (no restart channel).
//! - `command` — raw `forward_cmd` / `restart_cmd` shell templates, rendered
//!   per node. This is the single escape hatch for every transport: kubectl
//!   port-forward, ssh tunnels, or anything else that can forward a port and
//!   restart a process.

use std::net::SocketAddr;
use std::net::TcpListener;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use tokio::process::Child;
use tokio::process::Command;
use url::Url;

use crate::provider::NodeInfo;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    Direct,
    Command,
}

impl ProviderKind {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "direct" => Ok(Self::Direct),
            "command" => Ok(Self::Command),
            other => bail!("unknown provider '{other}' (expected direct or command)"),
        }
    }
}

/// Fully resolved provider settings (manifest `[provider]` block merged with
/// CLI flag overrides).
#[derive(Debug, Clone)]
pub struct OperationProvider {
    pub kind: ProviderKind,
    /// Raw per-node shell templates for the `command` provider.
    pub forward_cmd: Option<String>,
    pub restart_cmd: Option<String>,
    /// How long to wait for a forwarded local port to accept connections.
    pub forward_ready: Duration,
}

impl Default for OperationProvider {
    fn default() -> Self {
        Self {
            kind: ProviderKind::Direct,
            forward_cmd: None,
            restart_cmd: None,
            forward_ready: Duration::from_secs(20),
        }
    }
}

impl OperationProvider {
    /// Validate that the settings the chosen kind requires are present, before
    /// any node is touched.
    pub fn validate(&self, restart_needed: bool) -> Result<()> {
        match self.kind {
            ProviderKind::Direct => {}
            ProviderKind::Command => {
                if self.forward_cmd.is_none() {
                    bail!("--forward-cmd is required for the command provider");
                }
                if restart_needed && self.restart_cmd.is_none() {
                    bail!("--restart-cmd is required for the command provider on restart");
                }
            }
        }
        Ok(())
    }

    /// True when this provider tunnels the admin plane (vs. hitting it directly).
    fn tunnels(&self) -> bool {
        !matches!(self.kind, ProviderKind::Direct)
    }

    /// Establish access to every node's admin plane. Returns effective
    /// `NodeInfo`s (with `admin_url` rewritten to the local forward for
    /// tunnelling providers) plus a guard that must be held for the duration of
    /// the operation — dropping it tears down every tunnel.
    pub async fn connect(&self, nodes: &[NodeInfo]) -> Result<AdminAccess> {
        if !self.tunnels() {
            return Ok(AdminAccess {
                nodes: nodes.to_vec(),
                _forwards: Vec::new(),
            });
        }
        let mut effective = Vec::with_capacity(nodes.len());
        let mut forwards = Vec::with_capacity(nodes.len());
        for node in nodes {
            let local_port = reserve_local_port().context("reserve local forward port")?;
            let cmd = self.forward_command(node, local_port)?;
            let forward = Forward::spawn(node, &cmd, local_port, self.forward_ready)
                .await
                .with_context(|| format!("open admin forward to node {}", node.id))?;
            let mut rewritten = node.clone();
            rewritten.admin_url = forward.local_url.clone();
            effective.push(rewritten);
            forwards.push(forward);
        }
        Ok(AdminAccess {
            nodes: effective,
            _forwards: forwards,
        })
    }

    /// Shell command that restarts `node`, or an error if the kind has no
    /// restart channel (`direct`).
    pub fn restart_command(&self, node: &NodeInfo) -> Result<String> {
        match self.kind {
            ProviderKind::Direct => bail!(
                "the direct provider has no restart channel; use the command provider with --restart-cmd"
            ),
            ProviderKind::Command => {
                let template = self.restart_cmd.as_deref().context("restart_cmd unset")?;
                Ok(render_node_template(template, node))
            }
        }
    }

    fn forward_command(&self, node: &NodeInfo, local_port: u16) -> Result<String> {
        match self.kind {
            ProviderKind::Direct => bail!("direct provider does not forward"),
            ProviderKind::Command => {
                let template = self
                    .forward_cmd
                    .as_deref()
                    .context("--forward-cmd is required for the command provider")?;
                Ok(render_forward_template(template, node, local_port))
            }
        }
    }
}

/// Effective node list plus tunnel guards. Keep it alive for the whole
/// operation; the `_forwards` field kills every subprocess on drop.
pub struct AdminAccess {
    pub nodes: Vec<NodeInfo>,
    _forwards: Vec<Forward>,
}

/// One live port-forward subprocess. `kill_on_drop` guarantees teardown even
/// if the operation panics.
struct Forward {
    local_url: Url,
    #[allow(dead_code, reason = "held only so Drop kills the subprocess")]
    child: Child,
}

impl Forward {
    async fn spawn(
        node: &NodeInfo,
        cmd: &str,
        local_port: u16,
        ready_timeout: Duration,
    ) -> Result<Self> {
        let child = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn admin forward: {cmd}"))?;
        let local_url = Url::parse(&format!("http://127.0.0.1:{local_port}"))
            .context("compose local forward url")?;
        let addr: SocketAddr = ([127, 0, 0, 1], local_port).into();
        wait_for_local_port(addr, ready_timeout)
            .await
            .with_context(|| format!("admin forward to node {} never became reachable", node.id))?;
        Ok(Self { local_url, child })
    }
}

fn render_forward_template(template: &str, node: &NodeInfo, local_port: u16) -> String {
    render_node_template(template, node)
        .replace("{local_port}", &local_port.to_string())
        .replace("{admin_port}", &node.admin_port().to_string())
        .replace(
            "{admin_host}",
            node.admin_url.host_str().unwrap_or("127.0.0.1"),
        )
}

fn render_node_template(template: &str, node: &NodeInfo) -> String {
    template
        .replace("{node_id}", &node.id.to_string())
        .replace("{host}", &node.host)
        .replace("{instance_id}", node.instance_id.as_deref().unwrap_or(""))
        .replace(
            "{http_url}",
            node.http_url.as_ref().map(Url::as_str).unwrap_or(""),
        )
        .replace(
            "{name}",
            node.name.as_deref().unwrap_or(&node.id.to_string()),
        )
}

/// Bind an ephemeral port, then release it so the forwarder can claim it. There
/// is an unavoidable TOCTOU window here, but the alternative (parsing forwarder
/// stdout for the chosen port) is tool-specific; this matches how most
/// port-forward wrappers pick a local port.
fn reserve_local_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).context("bind ephemeral port")?;
    let port = listener.local_addr().context("read ephemeral port")?.port();
    Ok(port)
}

async fn wait_for_local_port(addr: SocketAddr, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if Instant::now() >= deadline {
                    bail!("port {addr} not reachable within {timeout:?}: {err}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> NodeInfo {
        NodeInfo {
            id: 7,
            admin_url: Url::parse("http://127.0.0.1:4438").unwrap(),
            host: "10.0.0.7".to_owned(),
            instance_id: Some("i-0abc".to_owned()),
            http_url: Some(Url::parse("http://10.0.0.7:4437").unwrap()),
            name: Some("node-7".to_owned()),
        }
    }

    #[test]
    fn direct_has_no_restart_channel() {
        let p = OperationProvider::default();
        assert!(p.restart_command(&node()).is_err());
    }

    #[test]
    fn command_provider_renders_templates() {
        let p = OperationProvider {
            kind: ProviderKind::Command,
            forward_cmd: Some("mytunnel {name} {host} {local_port} {admin_port}".to_owned()),
            restart_cmd: Some("myrestart {instance_id}".to_owned()),
            ..Default::default()
        };
        assert_eq!(
            p.forward_command(&node(), 40001).unwrap(),
            "mytunnel node-7 10.0.0.7 40001 4438"
        );
        assert_eq!(p.restart_command(&node()).unwrap(), "myrestart i-0abc");
    }

    #[test]
    fn validate_reports_missing_requirements() {
        let mut p = OperationProvider {
            kind: ProviderKind::Command,
            ..Default::default()
        };
        assert!(p.validate(false).is_err()); // missing forward_cmd
        p.forward_cmd = Some("mytunnel {host} {local_port}".to_owned());
        assert!(p.validate(false).is_ok()); // observe needs no restart channel
        assert!(p.validate(true).is_err()); // restart → restart_cmd required
        p.restart_cmd = Some("myrestart {name}".to_owned());
        assert!(p.validate(true).is_ok());
    }
}
