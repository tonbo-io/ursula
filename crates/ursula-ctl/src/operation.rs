//! Operation providers: how ursulactl reaches each node's loopback-bound admin
//! plane and restarts it. Nodes carry no cluster-mutation surface on the
//! network, so every operator request is tunnelled and every restart runs
//! through the same transport.
//!
//! Providers are *named*, not hand-assembled: the caller picks a [`ProviderKind`]
//! and a few structured values, and each provider builds its own forward and
//! restart shell commands. The `command` kind is the escape hatch that takes
//! raw templates for transports the named set does not cover.
//!
//! - `direct`  — admin URL already reachable; observe-only (no restart channel).
//! - `ssh`     — `ssh -N -L` tunnel; `ssh … systemctl restart <unit>`.
//! - `eice`    — ssh over AWS EC2 Instance Connect (send-key + open-tunnel,
//!   addressed by instance id); same restart as ssh.
//! - `command` — raw `forward_cmd` / `restart_cmd` templates.

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
    Ssh,
    Eice,
    Command,
}

impl ProviderKind {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "direct" => Ok(Self::Direct),
            "ssh" => Ok(Self::Ssh),
            "eice" => Ok(Self::Eice),
            "command" => Ok(Self::Command),
            other => bail!("unknown provider '{other}' (expected direct, ssh, eice, or command)"),
        }
    }
}

/// Fully resolved provider settings (manifest `[provider]` block merged with
/// CLI flag overrides).
#[derive(Debug, Clone)]
pub struct OperationProvider {
    pub kind: ProviderKind,
    /// AWS region for `eice`.
    pub region: Option<String>,
    /// SSH login user for `ssh`/`eice`.
    pub ssh_user: Option<String>,
    /// SSH private key path for `ssh`/`eice`; `<key>.pub` is sent for `eice`.
    pub ssh_key: Option<String>,
    /// systemd unit restarted by `ssh`/`eice`.
    pub restart_unit: Option<String>,
    /// Raw templates for the `command` escape hatch.
    pub forward_cmd: Option<String>,
    pub restart_cmd: Option<String>,
    /// How long to wait for a forwarded local port to accept connections.
    pub forward_ready: Duration,
}

impl Default for OperationProvider {
    fn default() -> Self {
        Self {
            kind: ProviderKind::Direct,
            region: None,
            ssh_user: None,
            ssh_key: None,
            restart_unit: None,
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
            ProviderKind::Ssh => {
                if self.ssh_user.is_none() {
                    bail!("--ssh-user is required for the ssh provider");
                }
                if restart_needed && self.restart_unit.is_none() {
                    bail!("--restart-unit is required for the ssh provider on restart");
                }
            }
            ProviderKind::Eice => {
                if self.region.is_none() {
                    bail!("--region is required for the eice provider");
                }
                if self.ssh_user.is_none() {
                    bail!("--ssh-user is required for the eice provider");
                }
                if self.ssh_key.is_none() {
                    bail!(
                        "--ssh-key is required for the eice provider (its .pub is sent to EC2 Instance Connect)"
                    );
                }
                if restart_needed && self.restart_unit.is_none() {
                    bail!("--restart-unit is required for the eice provider on restart");
                }
            }
            ProviderKind::Command => {
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
                "the direct provider has no restart channel; pick ssh/eice/command for restart"
            ),
            ProviderKind::Ssh => {
                let unit = self.restart_unit.as_deref().context("restart_unit unset")?;
                Ok(format!(
                    "{} 'sudo systemctl restart {}'",
                    self.ssh_prefix(node.connect_target()),
                    shell_quote(unit),
                ))
            }
            ProviderKind::Eice => {
                let unit = self.restart_unit.as_deref().context("restart_unit unset")?;
                let id = node
                    .instance_id
                    .as_deref()
                    .with_context(|| format!("node {} has no instance_id for eice", node.id))?;
                Ok(format!(
                    "{} && {} 'sudo systemctl restart {}'",
                    self.eice_send_key(id)?,
                    self.eice_ssh_prefix(id)?,
                    shell_quote(unit),
                ))
            }
            ProviderKind::Command => {
                let template = self.restart_cmd.as_deref().context("restart_cmd unset")?;
                Ok(render_node_template(template, node))
            }
        }
    }

    fn forward_command(&self, node: &NodeInfo, local_port: u16) -> Result<String> {
        let admin_port = node.admin_port();
        match self.kind {
            ProviderKind::Direct => bail!("direct provider does not forward"),
            ProviderKind::Ssh => Ok(format!(
                "{} -N -L 127.0.0.1:{local_port}:127.0.0.1:{admin_port}",
                self.ssh_prefix(node.connect_target()),
            )),
            ProviderKind::Eice => {
                let id = node
                    .instance_id
                    .as_deref()
                    .with_context(|| format!("node {} has no instance_id for eice", node.id))?;
                Ok(format!(
                    "{} && {} -N -L 127.0.0.1:{local_port}:127.0.0.1:{admin_port}",
                    self.eice_send_key(id)?,
                    self.eice_ssh_prefix(id)?,
                ))
            }
            ProviderKind::Command => {
                let template = self
                    .forward_cmd
                    .as_deref()
                    .context("--forward-cmd is required for the command provider")?;
                Ok(render_forward_template(template, node, local_port))
            }
        }
    }

    /// `ssh [-i key] [-o …] user@target` — the shared prefix for the ssh provider.
    fn ssh_prefix(&self, target: &str) -> String {
        let user = self.ssh_user.as_deref().unwrap_or("");
        let mut s = String::from("ssh -o StrictHostKeyChecking=no -o BatchMode=yes");
        if let Some(key) = &self.ssh_key {
            s.push_str(&format!(" -i {}", shell_quote(key)));
        }
        if user.is_empty() {
            s.push_str(&format!(" {}", shell_quote(target)));
        } else {
            s.push_str(&format!(" {}@{}", shell_quote(user), shell_quote(target)));
        }
        s
    }

    /// EC2 Instance Connect one-time public-key push (valid ~60s).
    fn eice_send_key(&self, instance_id: &str) -> Result<String> {
        let region = self.region.as_deref().context("region unset")?;
        let user = self.ssh_user.as_deref().context("ssh_user unset")?;
        let key = self.ssh_key.as_deref().context("ssh_key unset")?;
        Ok(format!(
            "aws ec2-instance-connect send-ssh-public-key --region {} --instance-id {} \
             --instance-os-user {} --ssh-public-key file://{}.pub >/dev/null",
            shell_quote(region),
            shell_quote(instance_id),
            shell_quote(user),
            shell_quote(key),
        ))
    }

    /// `ssh -i key -o ProxyCommand="… open-tunnel …" user@instance-id`.
    fn eice_ssh_prefix(&self, instance_id: &str) -> Result<String> {
        let region = self.region.as_deref().context("region unset")?;
        let user = self.ssh_user.as_deref().context("ssh_user unset")?;
        let key = self.ssh_key.as_deref().context("ssh_key unset")?;
        Ok(format!(
            "ssh -o StrictHostKeyChecking=no -o BatchMode=yes -i {} \
             -o ProxyCommand=\"aws ec2-instance-connect open-tunnel --region {} --instance-id %h\" \
             {}@{}",
            shell_quote(key),
            shell_quote(region),
            shell_quote(user),
            shell_quote(instance_id),
        ))
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

/// Minimal single-quote shell escaping for values we splice into `sh -c`.
fn shell_quote(s: &str) -> String {
    if !s.is_empty()
        && s.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/' | b'@' | b':')
        })
    {
        return s.to_owned();
    }
    format!("'{}'", s.replace('\'', "'\\''"))
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
            ssh_host: None,
            http_url: Some(Url::parse("http://10.0.0.7:4437").unwrap()),
            name: Some("node-7".to_owned()),
        }
    }

    fn ssh_provider() -> OperationProvider {
        OperationProvider {
            kind: ProviderKind::Ssh,
            ssh_user: Some("ec2-user".to_owned()),
            restart_unit: Some("ursula.service".to_owned()),
            ..Default::default()
        }
    }

    #[test]
    fn ssh_forward_and_restart_commands() {
        let p = ssh_provider();
        let fwd = p.forward_command(&node(), 55001).unwrap();
        assert_eq!(
            fwd,
            "ssh -o StrictHostKeyChecking=no -o BatchMode=yes ec2-user@10.0.0.7 -N -L 127.0.0.1:55001:127.0.0.1:4438"
        );
        let restart = p.restart_command(&node()).unwrap();
        assert_eq!(
            restart,
            "ssh -o StrictHostKeyChecking=no -o BatchMode=yes ec2-user@10.0.0.7 'sudo systemctl restart ursula.service'"
        );
    }

    #[test]
    fn eice_forward_sends_key_then_tunnels_by_instance_id() {
        let p = OperationProvider {
            kind: ProviderKind::Eice,
            region: Some("us-east-1".to_owned()),
            ssh_user: Some("ec2-user".to_owned()),
            ssh_key: Some("/k/id".to_owned()),
            restart_unit: Some("ursula-chaos.service".to_owned()),
            ..Default::default()
        };
        let fwd = p.forward_command(&node(), 55002).unwrap();
        assert!(
            fwd.contains("send-ssh-public-key --region us-east-1 --instance-id i-0abc"),
            "{fwd}"
        );
        assert!(fwd.contains("file:///k/id.pub"), "{fwd}");
        assert!(fwd.contains("open-tunnel --region us-east-1"), "{fwd}");
        assert!(
            fwd.ends_with("ec2-user@i-0abc -N -L 127.0.0.1:55002:127.0.0.1:4438"),
            "{fwd}"
        );
        let restart = p.restart_command(&node()).unwrap();
        assert!(
            restart.ends_with("'sudo systemctl restart ursula-chaos.service'"),
            "{restart}"
        );
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
            forward_cmd: Some("mytunnel {host} {local_port} {admin_port}".to_owned()),
            restart_cmd: Some("myrestart {instance_id}".to_owned()),
            ..Default::default()
        };
        assert_eq!(
            p.forward_command(&node(), 40001).unwrap(),
            "mytunnel 10.0.0.7 40001 4438"
        );
        assert_eq!(p.restart_command(&node()).unwrap(), "myrestart i-0abc");
    }

    #[test]
    fn validate_reports_missing_requirements() {
        let mut p = OperationProvider {
            kind: ProviderKind::Eice,
            ..Default::default()
        };
        assert!(p.validate(true).is_err()); // missing region/user/key
        p.region = Some("us-east-1".to_owned());
        p.ssh_user = Some("ec2-user".to_owned());
        p.ssh_key = Some("/k/id".to_owned());
        assert!(p.validate(false).is_ok()); // no restart → unit not required
        assert!(p.validate(true).is_err()); // restart → unit required
    }
}
