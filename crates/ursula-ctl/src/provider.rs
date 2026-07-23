use std::path::Path;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::Serialize;
use url::Url;

/// Default admin-plane port; must match `server.admin_listen`'s default.
const DEFAULT_ADMIN_PORT: u16 = 4438;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u64,
    /// Admin-plane endpoint carrying the operator surface (raft ops,
    /// maintenance drain, metrics). ursulactl sends every request here, never
    /// to the public client plane. Nodes bind this to loopback, so under a
    /// tunnelling provider only its port matters; under `direct` it must be
    /// network-reachable.
    pub admin_url: Url,
    /// Generic address (hostname or IP) used by the `ssh`/`command` providers
    /// and `{host}` interpolation. Defaults to the admin URL's host.
    pub host: String,
    /// AWS instance id, required by the `eice`/`ssm` providers.
    #[serde(default)]
    pub instance_id: Option<String>,
    /// Explicit SSH target when it differs from `host` (e.g. a bastion alias).
    #[serde(default)]
    pub ssh_host: Option<String>,
    /// Optional public client-plane URL; kept for `{http_url}` interpolation in
    /// the `command` provider. ursulactl itself never sends operator traffic here.
    #[serde(default)]
    pub http_url: Option<Url>,
    #[serde(default)]
    pub name: Option<String>,
}

impl NodeInfo {
    /// The address the `ssh` provider connects to: explicit `ssh_host`, else
    /// `host`. (The `eice` provider addresses by `instance_id` instead.)
    pub fn connect_target(&self) -> &str {
        self.ssh_host.as_deref().unwrap_or(&self.host)
    }

    /// Admin-plane port; the tunnelling providers forward a local port to it.
    pub fn admin_port(&self) -> u16 {
        self.admin_url.port().unwrap_or(DEFAULT_ADMIN_PORT)
    }
}

/// Optional `[provider]` block: how ursulactl reaches this cluster. Flags on the
/// command line override any field set here.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawProvider {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub ssh_user: Option<String>,
    #[serde(default)]
    pub ssh_key: Option<String>,
    #[serde(default)]
    pub restart_unit: Option<String>,
    #[serde(default)]
    pub forward_cmd: Option<String>,
    #[serde(default)]
    pub restart_cmd: Option<String>,
}

#[allow(async_fn_in_trait)]
pub trait NodeProvider {
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>>;
}

/// File-backed manifest. Accepts TOML, JSON, or YAML (chosen by extension),
/// carrying an optional `[provider]` block plus the node list. The JSON form
/// stays tolerant of the legacy `scripts/ursula_ec2.py` shapes (a bare array,
/// or `public_ip`/`private_ip` + `http_port`).
#[derive(Debug, Clone)]
pub struct StaticNodeProvider {
    nodes: Vec<NodeInfo>,
    provider: Option<RawProvider>,
}

impl StaticNodeProvider {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path == Path::new("-") {
            let mut text = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut text)
                .context("read manifest from stdin")?;
            return Self::from_text_sniffed(&text);
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read node config {}", path.display()))?;
        let raw = match path.extension().and_then(|e| e.to_str()) {
            Some("toml") => RawConfig::Wrapped(
                toml::from_str(&text).with_context(|| format!("parse TOML {}", path.display()))?,
            ),
            Some("yaml") | Some("yml") => RawConfig::Wrapped(
                yaml_serde::from_str(&text)
                    .with_context(|| format!("parse YAML {}", path.display()))?,
            ),
            Some("json") | None => serde_json::from_str(&text)
                .with_context(|| format!("parse JSON {}", path.display()))?,
            Some(other) => bail!(
                "unsupported manifest extension '.{other}' for {}",
                path.display()
            ),
        };
        Self::from_raw(raw).with_context(|| format!("load node config {}", path.display()))
    }

    /// Parse a JSON manifest from bytes (back-compat entry point / tests).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::from_raw(serde_json::from_slice(bytes)?)
    }

    /// Parse a manifest with no filename to dispatch on (stdin). `{` is
    /// unambiguously JSON. A leading `[` is tried as JSON first but may be a
    /// TOML table header (`[provider]`, `[[nodes]]`), so it falls through.
    /// Everything else is tried as TOML, then YAML.
    fn from_text_sniffed(text: &str) -> Result<Self> {
        let trimmed = text.trim_start();
        if trimmed.starts_with('{') {
            return Self::from_raw(
                serde_json::from_str(text).context("parse JSON manifest from stdin")?,
            );
        }
        if trimmed.starts_with('[')
            && let Ok(raw) = serde_json::from_str(text)
        {
            return Self::from_raw(raw);
        }
        if let Ok(parsed) = toml::from_str(text) {
            return Self::from_raw(RawConfig::Wrapped(parsed));
        }
        let parsed =
            yaml_serde::from_str(text).context("parse manifest from stdin as TOML or YAML")?;
        Self::from_raw(RawConfig::Wrapped(parsed))
    }

    pub fn from_nodes(nodes: Vec<NodeInfo>) -> Self {
        Self {
            nodes,
            provider: None,
        }
    }

    fn from_raw(raw: RawConfig) -> Result<Self> {
        let (provider, nodes) = raw.into_parts()?;
        Ok(Self { nodes, provider })
    }

    /// The `[provider]` block, if the manifest declared one.
    pub fn provider_config(&self) -> Option<&RawProvider> {
        self.provider.as_ref()
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
    fn into_parts(self) -> Result<(Option<RawProvider>, Vec<NodeInfo>)> {
        match self {
            RawConfig::Wrapped(config) => {
                let default_port = config.http_port.or(config.port);
                let nodes = config
                    .nodes
                    .into_iter()
                    .map(|node| node.into_node_with_default_port(default_port))
                    .collect::<Result<_>>()?;
                Ok((config.provider, nodes))
            }
            RawConfig::Bare(nodes) => {
                let nodes = nodes
                    .into_iter()
                    .map(RawNode::into_node)
                    .collect::<Result<_>>()?;
                Ok((None, nodes))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawWrappedConfig {
    #[serde(default)]
    provider: Option<RawProvider>,
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
    admin_url: Option<String>,
    #[serde(default)]
    admin_port: Option<u16>,
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default)]
    ssh_host: Option<String>,
    #[serde(default)]
    http_url: Option<String>,
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
        // Resolve an optional public client URL and a generic host string.
        let (http_url, host_from_url) =
            if let Some(url) = self.http_url.as_deref().and_then(non_empty) {
                let parsed = Url::parse(url)
                    .with_context(|| format!("invalid http_url for node {}", self.id))?;
                let host = parsed.host_str().map(str::to_owned);
                (Some(parsed), host)
            } else if let Some(host_ip) = self
                .public_ip
                .as_deref()
                .and_then(non_empty)
                .or_else(|| self.private_ip.as_deref().and_then(non_empty))
            {
                let port = self
                    .http_port
                    .or(self.port)
                    .or(default_port)
                    .unwrap_or(8080);
                let parsed = Url::parse(&format!("http://{host_ip}:{port}"))
                    .with_context(|| format!("synthesize http_url for node {}", self.id))?;
                (Some(parsed), Some(host_ip.to_owned()))
            } else {
                (None, None)
            };

        let host = self
            .host
            .as_deref()
            .and_then(non_empty)
            .map(str::to_owned)
            .or(host_from_url)
            .or_else(|| {
                self.admin_url.as_deref().and_then(non_empty).and_then(|u| {
                    Url::parse(u)
                        .ok()
                        .and_then(|p| p.host_str().map(str::to_owned))
                })
            })
            .or_else(|| self.instance_id.clone())
            .with_context(|| {
                format!(
                    "node {} needs host, http_url, public_ip/private_ip, or instance_id",
                    self.id
                )
            })?;

        let admin_url = if let Some(url) = self.admin_url.as_deref().and_then(non_empty) {
            Url::parse(url).with_context(|| format!("invalid admin_url for node {}", self.id))?
        } else {
            let admin_port = self.admin_port.unwrap_or(DEFAULT_ADMIN_PORT);
            Url::parse(&format!("http://{host}:{admin_port}"))
                .with_context(|| format!("synthesize admin_url for node {}", self.id))?
        };
        Ok(NodeInfo {
            id: self.id,
            admin_url,
            host,
            instance_id: self
                .instance_id
                .and_then(|s| non_empty(&s).map(str::to_owned)),
            ssh_host: self.ssh_host.and_then(|s| non_empty(&s).map(str::to_owned)),
            http_url,
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
    async fn json_bare_array_back_compat() {
        let json = br#"[{"id":2,"public_ip":"203.0.113.10","http_port":9090,"name":"n2"}]"#;
        let provider = StaticNodeProvider::from_bytes(json).unwrap();
        let nodes = provider.list_nodes().await.unwrap();
        assert_eq!(
            nodes[0].http_url.as_ref().unwrap().as_str(),
            "http://203.0.113.10:9090/"
        );
        assert_eq!(nodes[0].host, "203.0.113.10");
        // admin_url defaults to host:4438.
        assert_eq!(nodes[0].admin_url.as_str(), "http://203.0.113.10:4438/");
        assert!(provider.provider_config().is_none());
    }

    #[test]
    fn stdin_sniffing_dispatches_on_content() {
        let json = r#"{"nodes":[{"id":1,"public_ip":"10.0.0.1"}]}"#;
        let provider = StaticNodeProvider::from_text_sniffed(json).unwrap();
        assert_eq!(provider.nodes.len(), 1);

        let bare_array = r#"[{"id":1,"public_ip":"10.0.0.1"}]"#;
        let provider = StaticNodeProvider::from_text_sniffed(bare_array).unwrap();
        assert_eq!(provider.nodes.len(), 1);

        let toml = "[[nodes]]\nid = 1\nhost = \"10.0.0.1\"\n";
        let provider = StaticNodeProvider::from_text_sniffed(toml).unwrap();
        assert_eq!(provider.nodes.len(), 1);

        let yaml = "nodes:\n  - id: 1\n    host: 10.0.0.1\n";
        let provider = StaticNodeProvider::from_text_sniffed(yaml).unwrap();
        assert_eq!(provider.nodes.len(), 1);

        assert!(StaticNodeProvider::from_text_sniffed("not a manifest").is_err());
    }

    #[tokio::test]
    async fn json_wrapped_port_alias_back_compat() {
        let json = br#"{"port":4491,"nodes":[{"id":3,"public_ip":"","private_ip":"10.0.0.3"}]}"#;
        let provider = StaticNodeProvider::from_bytes(json).unwrap();
        let nodes = provider.list_nodes().await.unwrap();
        assert_eq!(nodes[0].host, "10.0.0.3");
        assert_eq!(nodes[0].admin_url.as_str(), "http://10.0.0.3:4438/");
    }

    #[test]
    fn toml_manifest_with_provider_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.toml");
        std::fs::write(
            &path,
            r#"
[provider]
kind = "eice"
region = "us-east-1"
restart_unit = "ursula-chaos.service"

[[nodes]]
id = 1
instance_id = "i-0abc"
admin_port = 4438

[[nodes]]
id = 2
instance_id = "i-0def"
"#,
        )
        .unwrap();
        let provider = StaticNodeProvider::from_path(&path).unwrap();
        let pc = provider.provider_config().expect("provider block");
        assert_eq!(pc.kind.as_deref(), Some("eice"));
        assert_eq!(pc.region.as_deref(), Some("us-east-1"));
        assert_eq!(pc.restart_unit.as_deref(), Some("ursula-chaos.service"));
        let nodes = provider.nodes.clone();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].instance_id.as_deref(), Some("i-0abc"));
        assert_eq!(nodes[0].connect_target(), "i-0abc");
        assert_eq!(nodes[0].admin_port(), 4438);
        // host falls back to the instance id when nothing else is given.
        assert_eq!(nodes[0].host, "i-0abc");
    }

    #[test]
    fn unknown_provider_field_is_rejected() {
        // Node/top-level keys stay tolerant (shared with ursula_ec2.py's file),
        // but the provider block is our own schema and catches typos.
        let json =
            br#"{"provider":{"kind":"ssh","bogus":true},"nodes":[{"id":1,"host":"10.0.0.1"}]}"#;
        assert!(StaticNodeProvider::from_bytes(json).is_err());
    }
}
