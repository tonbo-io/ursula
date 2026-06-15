use std::path::Path;

use thiserror::Error;

use crate::config::UrsulaConfig;
use crate::preset::Preset;
use crate::validate::ValidationError;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config file not found: {0}")]
    NotFound(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML parse error: {0}")]
    TomlParse(#[from] toml::de::Error),
    #[error("YAML parse error: {0}")]
    YamlParse(#[from] yaml_serde::Error),
    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),
    #[error("{0}")]
    Other(String),
}

/// Search for a default config file when `--config` is not given.
///
/// Searches for TOML files first, then JSON, then YAML, in order of specificity.
pub fn find_default_config() -> Option<std::path::PathBuf> {
    let mut candidates = vec![
        std::path::PathBuf::from("./ursula.toml"),
        std::path::PathBuf::from("./ursula.json"),
        std::path::PathBuf::from("./ursula.yaml"),
        std::path::PathBuf::from("./ursula.yml"),
        std::path::PathBuf::from("/etc/ursula/ursula.toml"),
        std::path::PathBuf::from("/etc/ursula/ursula.json"),
        std::path::PathBuf::from("/etc/ursula/ursula.yaml"),
        std::path::PathBuf::from("/etc/ursula/ursula.yml"),
    ];
    if let Some(config_dir) = dirs::config_dir() {
        candidates.push(config_dir.join("ursula").join("config.toml"));
        candidates.push(config_dir.join("ursula").join("config.json"));
        candidates.push(config_dir.join("ursula").join("config.yaml"));
    }
    for path in &candidates {
        if path.exists() {
            return Some(path.clone());
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigFormat {
    Toml,
    Json,
    Yaml,
}

/// Parse raw config text into a `toml::Table` according to the given format.
fn parse_raw(raw: &str, format: ConfigFormat) -> Result<toml::Table, ConfigError> {
    match format {
        ConfigFormat::Toml => Ok(raw.parse()?),
        ConfigFormat::Json => {
            let value: toml::Value = serde_json::from_str(raw)?;
            value
                .as_table()
                .cloned()
                .ok_or_else(|| ConfigError::Other("top-level config value is not a mapping".into()))
        }
        ConfigFormat::Yaml => {
            let value: toml::Value = yaml_serde::from_str(raw)?;
            value
                .as_table()
                .cloned()
                .ok_or_else(|| ConfigError::Other("top-level config value is not a mapping".into()))
        }
    }
}

/// Load config from an optional file path with optional preset and node_id override.
///
/// `node_id` may be set in the config file as `raft.node_id`; the CLI
/// `--node-id` flag intentionally overrides it for per-node identity or
/// deployment-derived identities such as StatefulSet ordinals.
///
/// When `path` is `None`, the config is built entirely from the preset (if any)
/// plus `UrsulaConfig` defaults.  This allows `--preset tiny` to work without
/// a config file.
///
/// The file format is detected from the extension:
/// * `.toml`  → TOML
/// * `.json`  → JSON
/// * `.yaml` / `.yml` → YAML
/// * anything else → error
pub fn load_config(
    path: Option<&Path>,
    preset: Option<Preset>,
    node_id: Option<u64>,
) -> Result<UrsulaConfig, ConfigError> {
    let user_table = match path {
        Some(path) => {
            let raw = std::fs::read_to_string(path)?;
            let format = match path.extension().and_then(|e| e.to_str()) {
                Some("yaml") | Some("yml") => ConfigFormat::Yaml,
                Some("json") => ConfigFormat::Json,
                Some("toml") => ConfigFormat::Toml,
                _ => {
                    return Err(ConfigError::Other(format!(
                        "unsupported config file extension for '{}'",
                        path.display()
                    )));
                }
            };
            parse_raw(&raw, format)?
        }
        None => toml::Table::new(),
    };

    let mut base_table = match preset {
        Some(p) => {
            let preset_config = UrsulaConfig::from(p);
            toml::Value::try_from(preset_config)
                .map_err(|e| ConfigError::Other(format!("serialize preset: {e}")))?
                .as_table()
                .cloned()
                .ok_or_else(|| ConfigError::Other("preset is not a table".into()))?
        }
        None => toml::Table::new(),
    };

    merge_tables(&mut base_table, user_table);

    let mut config: UrsulaConfig = base_table.try_into()?;
    if let Some(id) = node_id {
        config.raft.node_id = id;
    }
    if config.raft.init_membership_per_group {
        config.raft.init_membership = true;
    }
    config.validate()?;
    Ok(config)
}

/// Deep-merge two TOML tables.
///
/// * Tables are merged recursively (user keys override base keys).
/// * Arrays are replaced wholesale (user array wins).
/// * Scalar values are overwritten.
///
/// This is the standard "preset + user override" semantics: the preset
/// supplies the base configuration and the user's TOML file patches it.
/// We operate at the `toml::Table` AST level because serde does not
/// provide a way to partially-deserialise into an existing struct.
fn merge_tables(base: &mut toml::Table, user: toml::Table) {
    for (key, user_value) in user {
        match base.get_mut(&key) {
            Some(toml::Value::Table(base_sub)) => {
                if let toml::Value::Table(user_sub) = user_value {
                    merge_tables(base_sub, user_sub);
                    continue;
                }
            }
            Some(toml::Value::Array(base_arr)) => {
                if let toml::Value::Array(user_arr) = user_value {
                    // Arrays: user array replaces base array (TOML semantics)
                    *base_arr = user_arr;
                    continue;
                }
            }
            _ => {}
        }
        base.insert(key, user_value);
    }
}

#[cfg(test)]
pub fn merge_tables_for_test(base: &mut toml::Table, user: toml::Table) {
    merge_tables(base, user);
}
