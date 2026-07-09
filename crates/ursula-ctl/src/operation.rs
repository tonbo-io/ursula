//! Operation providers: how ursulactl reaches each node's loopback-bound admin
//! plane. Nodes carry no cluster-mutation surface on the network, so every
//! operator request is tunnelled. `direct` assumes the admin URL is already
//! reachable (an outer tunnel, a lab bind, or `127.0.0.1` for a local node);
//! `forward` spawns one port-forward subprocess per node — SSH, SSM, and
//! `kubectl port-forward` all reduce to the same shape and differ only in the
//! command template.

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

/// How to reach node admin planes.
#[derive(Debug, Clone)]
pub enum OperationProvider {
    /// Hit `node.admin_url` as configured. Use when it is already reachable.
    Direct,
    /// Spawn a port-forward subprocess per node and rewrite each admin_url to
    /// the local forwarded address. The template is rendered per node with
    /// `{local_port}`, `{admin_port}`, `{admin_host}`, `{host}`, `{node_id}`,
    /// and `{name}` before running under `sh -c`.
    ///
    /// Examples:
    /// - SSH:  `ssh -N -L {local_port}:127.0.0.1:{admin_port} ec2-user@{host}`
    /// - SSM:  `aws ssm start-session --target {name} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters host=127.0.0.1,portNumber={admin_port},localPortNumber={local_port}`
    /// - kube: `kubectl port-forward pod/{name} {local_port}:{admin_port}`
    Forward {
        template: String,
        /// How long to wait for the forwarded local port to accept a connection.
        ready_timeout: Duration,
    },
}

impl OperationProvider {
    /// Establish access to every node's admin plane. Returns effective
    /// `NodeInfo`s (with `admin_url` rewritten to the local forward under
    /// `Forward`) plus a guard that must be held for the duration of the
    /// operation — dropping it tears down every tunnel.
    pub async fn connect(&self, nodes: &[NodeInfo]) -> Result<AdminAccess> {
        match self {
            OperationProvider::Direct => Ok(AdminAccess {
                nodes: nodes.to_vec(),
                _forwards: Vec::new(),
            }),
            OperationProvider::Forward {
                template,
                ready_timeout,
            } => {
                let mut effective = Vec::with_capacity(nodes.len());
                let mut forwards = Vec::with_capacity(nodes.len());
                for node in nodes {
                    let forward = Forward::spawn(node, template, *ready_timeout)
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
    async fn spawn(node: &NodeInfo, template: &str, ready_timeout: Duration) -> Result<Self> {
        let local_port = reserve_local_port().context("reserve local forward port")?;
        let admin_port = node.admin_url.port().unwrap_or(4438);
        let admin_host = node.admin_url.host_str().unwrap_or("127.0.0.1");
        let rendered = render_forward(template, local_port, admin_port, admin_host, node);
        let child = Command::new("sh")
            .arg("-c")
            .arg(&rendered)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn admin forward: {rendered}"))?;
        let local_url = Url::parse(&format!("http://127.0.0.1:{local_port}"))
            .context("compose local forward url")?;

        let addr: SocketAddr = ([127, 0, 0, 1], local_port).into();
        wait_for_local_port(addr, ready_timeout)
            .await
            .with_context(|| format!("admin forward to node {} never became reachable", node.id))?;
        Ok(Self { local_url, child })
    }
}

fn render_forward(
    template: &str,
    local_port: u16,
    admin_port: u16,
    admin_host: &str,
    node: &NodeInfo,
) -> String {
    template
        .replace("{local_port}", &local_port.to_string())
        .replace("{admin_port}", &admin_port.to_string())
        .replace("{admin_host}", admin_host)
        .replace("{host}", &node.host)
        .replace("{node_id}", &node.id.to_string())
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
            http_url: Url::parse("http://10.0.0.7:4437").unwrap(),
            admin_url: Url::parse("http://10.0.0.7:4438").unwrap(),
            host: "10.0.0.7".to_owned(),
            name: Some("node-7".to_owned()),
        }
    }

    #[test]
    fn render_forward_fills_ssh_template() {
        let rendered = render_forward(
            "ssh -N -L {local_port}:127.0.0.1:{admin_port} ec2-user@{host}",
            55001,
            4438,
            "10.0.0.7",
            &node(),
        );
        assert_eq!(rendered, "ssh -N -L 55001:127.0.0.1:4438 ec2-user@10.0.0.7");
    }

    #[test]
    fn render_forward_fills_kube_template() {
        let rendered = render_forward(
            "kubectl port-forward pod/{name} {local_port}:{admin_port}",
            55002,
            4438,
            "127.0.0.1",
            &node(),
        );
        assert_eq!(rendered, "kubectl port-forward pod/node-7 55002:4438");
    }

    #[tokio::test]
    async fn direct_provider_returns_nodes_unchanged() {
        let nodes = vec![node()];
        let access = OperationProvider::Direct.connect(&nodes).await.unwrap();
        assert_eq!(access.nodes.len(), 1);
        assert_eq!(access.nodes[0].admin_url.as_str(), "http://10.0.0.7:4438/");
    }
}
