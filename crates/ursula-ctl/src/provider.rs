use std::path::Path;

use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use url::Url;

/// Default admin-plane port; must match `server.admin_listen`'s default.
const DEFAULT_ADMIN_PORT: u16 = 4438;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u64,
    pub http_url: Url,
    /// Admin-plane endpoint carrying the operator surface (raft ops,
    /// maintenance drain, metrics). ursulactl sends every request here, never
    /// to the public client plane. Nodes bind this to loopback, so this URL is
    /// typically reached through a tunnel an [`OperationProvider`] sets up.
    pub admin_url: Url,
    /// Host string used for {host} template interpolation in --restart-cmd and
    /// forward commands. Typically equals http_url's host but may differ when
    /// ursulactl talks to a public address while commands target a private one.
    pub host: String,
    #[serde(default)]
    pub name: Option<String>,
}

#[allow(async_fn_in_trait)]
pub trait NodeProvider {
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>>;
}

/// File-backed provider. The JSON shape is intentionally tolerant of the
/// `scripts/ursula_ec2.py` `nodes.json` already in use: it accepts either an
/// explicit `http_url` or the legacy `public_ip`/`private_ip` + `http_port`
/// combination.
#[derive(Debug, Clone)]
pub struct StaticNodeProvider {
    nodes: Vec<NodeInfo>,
}

impl StaticNodeProvider {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes =
            std::fs::read(path).with_context(|| format!("read node config {}", path.display()))?;
        Self::from_bytes(&bytes).with_context(|| format!("parse node config {}", path.display()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let raw: RawConfig = serde_json::from_slice(bytes)?;
        let nodes = raw.into_nodes()?;
        Ok(Self { nodes })
    }

    pub fn from_nodes(nodes: Vec<NodeInfo>) -> Self {
        Self { nodes }
    }
}

impl NodeProvider for StaticNodeProvider {
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        Ok(self.nodes.clone())
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawConfig {
    Wrapped(RawWrappedConfig),
    Bare(Vec<RawNode>),
}

impl RawConfig {
    fn into_nodes(self) -> Result<Vec<NodeInfo>> {
        let raws = match self {
            RawConfig::Wrapped(config) => {
                let default_port = config.http_port.or(config.port);
                return config
                    .nodes
                    .into_iter()
                    .map(|node| node.into_node_with_default_port(default_port))
                    .collect();
            }
            RawConfig::Bare(nodes) => nodes,
        };
        raws.into_iter().map(RawNode::into_node).collect()
    }
}

#[derive(Debug, Deserialize)]
struct RawWrappedConfig {
    nodes: Vec<RawNode>,
    #[serde(default)]
    http_port: Option<u16>,
    #[serde(default)]
    port: Option<u16>,
}

#[derive(Debug, Deserialize)]
struct RawNode {
    id: u64,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    http_url: Option<String>,
    #[serde(default)]
    admin_url: Option<String>,
    #[serde(default)]
    admin_port: Option<u16>,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    public_ip: Option<String>,
    #[serde(default)]
    private_ip: Option<String>,
    #[serde(default)]
    http_port: Option<u16>,
    #[serde(default)]
    port: Option<u16>,
}

impl RawNode {
    fn into_node(self) -> Result<NodeInfo> {
        self.into_node_with_default_port(None)
    }

    fn into_node_with_default_port(self, default_port: Option<u16>) -> Result<NodeInfo> {
        let (http_url, host) = if let Some(url) = self.http_url.as_deref() {
            let parsed = Url::parse(url)
                .with_context(|| format!("invalid http_url for node {}", self.id))?;
            let host = self
                .host
                .as_deref()
                .and_then(non_empty)
                .map(str::to_owned)
                .or_else(|| parsed.host_str().map(str::to_owned))
                .with_context(|| format!("node {} has no host", self.id))?;
            (parsed, host)
        } else {
            let host_ip = self
                .public_ip
                .as_deref()
                .and_then(non_empty)
                .or_else(|| self.private_ip.as_deref().and_then(non_empty))
                .with_context(|| {
                    format!("node {} requires http_url or public_ip/private_ip", self.id)
                })?;
            let port = self
                .http_port
                .or(self.port)
                .or(default_port)
                .unwrap_or(8080);
            let parsed = Url::parse(&format!("http://{host_ip}:{port}"))
                .with_context(|| format!("synthesize http_url for node {}", self.id))?;
            let host = self
                .host
                .as_deref()
                .and_then(non_empty)
                .unwrap_or(host_ip)
                .to_owned();
            (parsed, host)
        };
        let admin_url = if let Some(url) = self.admin_url.as_deref().and_then(non_empty) {
            Url::parse(url).with_context(|| format!("invalid admin_url for node {}", self.id))?
        } else {
            let admin_port = self.admin_port.unwrap_or(DEFAULT_ADMIN_PORT);
            Url::parse(&format!("http://{host}:{admin_port}"))
                .with_context(|| format!("synthesize admin_url for node {}", self.id))?
        };
        Ok(NodeInfo {
            id: self.id,
            http_url,
            admin_url,
            host,
            name: self.name,
        })
    }
}

fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_provider_accepts_explicit_url() {
        let json = br#"{"nodes":[{"id":1,"http_url":"http://10.0.0.5:8080","host":"10.0.0.5"}]}"#;
        let provider = StaticNodeProvider::from_bytes(json).unwrap();
        let nodes = provider.list_nodes().await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, 1);
        assert_eq!(nodes[0].host, "10.0.0.5");
        assert_eq!(nodes[0].http_url.as_str(), "http://10.0.0.5:8080/");
    }

    #[tokio::test]
    async fn static_provider_synthesizes_url_from_legacy_fields() {
        let json = br#"[{"id":2,"public_ip":"203.0.113.10","http_port":9090,"name":"n2"}]"#;
        let provider = StaticNodeProvider::from_bytes(json).unwrap();
        let nodes = provider.list_nodes().await.unwrap();
        assert_eq!(nodes[0].http_url.as_str(), "http://203.0.113.10:9090/");
        assert_eq!(nodes[0].host, "203.0.113.10");
        assert_eq!(nodes[0].name.as_deref(), Some("n2"));
    }

    #[tokio::test]
    async fn static_provider_accepts_bench_config_port_alias() {
        let json = br#"{"port":4491,"nodes":[{"id":3,"public_ip":"","private_ip":"10.0.0.3"}]}"#;
        let provider = StaticNodeProvider::from_bytes(json).unwrap();
        let nodes = provider.list_nodes().await.unwrap();
        assert_eq!(nodes[0].http_url.as_str(), "http://10.0.0.3:4491/");
        assert_eq!(nodes[0].host, "10.0.0.3");
    }
}
