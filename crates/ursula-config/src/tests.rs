#[cfg(test)]
mod human_tests {
    use std::time::Duration;

    use crate::human::HumanDuration;
    use crate::human::HumanSize;

    #[test]
    fn human_duration_from_str_seconds() {
        assert_eq!(
            "30s".parse::<HumanDuration>().unwrap().as_duration(),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn human_duration_from_str_milliseconds() {
        assert_eq!(
            "250ms".parse::<HumanDuration>().unwrap().as_duration(),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn human_duration_from_integer() {
        let raw: toml::Value = toml::Value::Integer(30000);
        let dur: HumanDuration = raw.try_into().unwrap();
        assert_eq!(dur.as_duration(), Duration::from_millis(30000));
    }

    #[test]
    fn human_size_from_str_mib() {
        assert_eq!(
            "256MiB".parse::<HumanSize>().unwrap().as_bytes(),
            256 * 1024 * 1024
        );
    }

    #[test]
    fn human_size_from_str_gib() {
        assert_eq!(
            "1.5GiB".parse::<HumanSize>().unwrap().as_bytes(),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
        );
    }

    #[test]
    fn human_size_from_integer() {
        let raw: toml::Value = toml::Value::Integer(67108864);
        let size: HumanSize = raw.try_into().unwrap();
        assert_eq!(size.as_bytes(), 67108864);
    }

    #[test]
    fn human_duration_display_roundtrip() {
        let dur = HumanDuration::sec(60);
        assert_eq!(dur.to_string(), "1m");
        assert_eq!(
            dur.to_string()
                .parse::<HumanDuration>()
                .unwrap()
                .as_duration(),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn human_duration_rejects_decimal_ms() {
        assert!("1.5ms".parse::<HumanDuration>().is_err());
    }

    #[test]
    fn human_duration_rejects_invalid_unit() {
        assert!("30x".parse::<HumanDuration>().is_err());
    }

    #[test]
    fn human_duration_rejects_negative() {
        assert!("-1s".parse::<HumanDuration>().is_err());
    }

    #[test]
    fn human_duration_parses_hours() {
        assert_eq!(
            "2h".parse::<HumanDuration>().unwrap().as_duration(),
            Duration::from_secs(7200)
        );
    }

    #[test]
    fn human_duration_parses_days() {
        assert_eq!(
            "1d".parse::<HumanDuration>().unwrap().as_duration(),
            Duration::from_secs(86400)
        );
    }

    #[test]
    fn human_size_display_roundtrip() {
        let size = HumanSize::gib(1);
        assert_eq!(size.to_string(), "1GiB");
        assert_eq!(
            size.to_string().parse::<HumanSize>().unwrap().as_bytes(),
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn human_size_rejects_invalid_unit() {
        assert!("30x".parse::<HumanSize>().is_err());
    }

    #[test]
    fn human_size_rejects_negative() {
        assert!("-1MiB".parse::<HumanSize>().is_err());
    }

    #[test]
    fn human_size_parses_all_units() {
        assert_eq!("100B".parse::<HumanSize>().unwrap().as_bytes(), 100);
        assert_eq!("1KiB".parse::<HumanSize>().unwrap().as_bytes(), 1024);
        assert_eq!("1MiB".parse::<HumanSize>().unwrap().as_bytes(), 1024 * 1024);
        assert_eq!(
            "1GiB".parse::<HumanSize>().unwrap().as_bytes(),
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn human_size_rejects_overflow() {
        assert!("99999999999999999999GiB".parse::<HumanSize>().is_err());
    }

    #[test]
    fn human_duration_from_toml_string() {
        let raw: toml::Value = toml::Value::String("5m".into());
        let dur: HumanDuration = raw.try_into().unwrap();
        assert_eq!(dur.as_duration(), Duration::from_secs(300));
    }

    #[test]
    fn human_size_from_toml_string() {
        let raw: toml::Value = toml::Value::String("128MiB".into());
        let size: HumanSize = raw.try_into().unwrap();
        assert_eq!(size.as_bytes(), 128 * 1024 * 1024);
    }
}

#[cfg(test)]
mod config_tests {
    use crate::config::UrsulaConfig;

    #[test]
    fn deserialize_minimal_config() {
        let toml = r#"
[server]
listen = "0.0.0.0:4437"

[runtime]
core_count = 16

[raft]
group_count = 256

[raft.wal]
backend = "disk"
path = "/var/lib/ursula"

[[raft.peers]]
node_id = 1
url = "http://10.0.0.1:4437"

[storage.cold]
backend = "s3"
flush_interval = "30s"
flush_size = "64MiB"

[storage.cold.s3]
bucket = "my-bucket"
region = "us-east-1"

[storage.snapshot]
backend = "s3"
"#;
        let config: UrsulaConfig = toml::from_str(toml).expect("valid config");
        assert_eq!(config.server.listen, "0.0.0.0:4437");
        assert_eq!(config.runtime.core_count, 16);
        // node_id is not in the file — it comes from CLI --node-id at runtime
        assert_eq!(config.raft.node_id, 0); // serde default
        assert_eq!(config.raft.group_count, 256);
        use crate::config::ColdBackend;
        use crate::config::RaftSnapshotBackend;
        use crate::config::WalBackend;
        assert_eq!(config.raft.wal.backend, WalBackend::Disk);
        assert_eq!(config.storage.cold.backend, ColdBackend::S3);
        assert_eq!(config.storage.snapshot.backend, RaftSnapshotBackend::S3);
        assert_eq!(
            config.storage.cold.s3.as_ref().unwrap().bucket,
            Some("my-bucket".into())
        );
    }
}

#[cfg(test)]
mod load_tests {
    use std::io::Write;

    use crate::config::WalBackend;
    use crate::load::load_config;
    use crate::preset::Preset;

    #[test]
    fn load_minimal_config_without_preset() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[server]
listen = "127.0.0.1:4437"

[runtime]
core_count = 4

[raft]
group_count = 16

[raft.wal]
backend = "memory"
"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.runtime.core_count, 4);
        assert_eq!(config.raft.node_id, 1);
        assert_eq!(config.raft.wal.backend, WalBackend::Memory);
    }

    #[test]
    fn preset_tiny_overrides_defaults() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[server]
listen = "127.0.0.1:4437"
"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), Some(Preset::Tiny), Some(1)).unwrap();
        assert_eq!(
            config.runtime.core_count,
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        ); // preset no longer overrides core_count
        assert_eq!(config.raft.group_count, 64); // from tiny preset
        assert_eq!(config.raft.wal.backend, WalBackend::Memory); // from tiny preset
    }

    #[test]
    fn user_config_overrides_preset() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[runtime]
core_count = 2
"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), Some(Preset::Tiny), Some(1)).unwrap();
        assert_eq!(config.runtime.core_count, 2); // user overrides preset's 4
        assert_eq!(config.raft.group_count, 64); // preset still applies
    }

    #[test]
    fn validation_rejects_disk_without_path() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[raft.wal]
backend = "disk"
"#
        )
        .unwrap();
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("raft.wal.path"),
            "error should mention raft.wal.path: {msg}"
        );
    }

    #[test]
    fn validation_rejects_s3_without_bucket() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[storage.cold]
backend = "s3"
"#
        )
        .unwrap();
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("bucket"), "error should mention bucket: {msg}");
    }

    #[test]
    fn nested_table_merge() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[storage.cold.cache]
max_size = "128MiB"
"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), Some(Preset::Tiny), Some(1)).unwrap();
        // tiny preset sets cache.max_size = "64MiB", user overrides to "128MiB"
        assert_eq!(
            config
                .storage
                .cold
                .cache
                .as_ref()
                .unwrap()
                .max_size
                .as_bytes(),
            128 * 1024 * 1024
        );
        // but tiny preset also sets flush_size = "4MiB", which should still apply
        assert_eq!(config.storage.cold.flush_size.as_bytes(), 4 * 1024 * 1024);
    }

    #[test]
    fn array_replacement_not_append() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[[raft.peers]]
node_id = 1
url = "http://10.0.0.1:4437"

[[raft.peers]]
node_id = 2
url = "http://10.0.0.2:4437"
"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.raft.peers.len(), 2);
        assert_eq!(config.raft.peers[0].node_id, 1);
        assert_eq!(config.raft.peers[1].node_id, 2);
    }

    #[test]
    fn node_id_from_cli_overrides_file() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[raft]
node_id = 1
"#
        )
        .unwrap();
        // CLI --node-id 42 overrides file's node_id = 1
        let config = load_config(Some(tmp.path()), None, Some(42)).unwrap();
        assert_eq!(config.raft.node_id, 42);
    }

    #[test]
    fn validation_rejects_missing_node_id() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".toml").unwrap();
        write!(
            tmp,
            r#"
[server]
listen = "127.0.0.1:4437"
"#
        )
        .unwrap();
        let err = load_config(Some(tmp.path()), None, None).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("node_id") && msg.contains("--node-id"),
            "error should mention --node-id: {msg}"
        );
    }

    /// Verify that a Default config survives a serde round-trip without
    /// information loss.  This is the foundation that makes `merge_tables`
    /// safe: preset defaults are serialised to a TOML table, merged with
    /// the user's partial TOML, and then deserialised back.
    #[test]
    fn preset_roundtrip_equality() {
        use crate::UrsulaConfig;
        use crate::preset::Preset;

        let preset = Preset::Standard;
        let original = UrsulaConfig::from(preset);

        // 1. Serialise preset to TOML AST
        let value = toml::Value::try_from(&original).expect("serialise");
        let table = value.as_table().cloned().expect("is table");

        // 2. Merge with an empty user table (no-op)
        let mut merged = table.clone();
        crate::load::merge_tables_for_test(&mut merged, toml::Table::new());

        // 3. Deserialise back
        let restored: UrsulaConfig = merged.try_into().expect("deserialise");

        // Scalar fields must be identical
        assert_eq!(original.server.listen, restored.server.listen);
        assert_eq!(original.runtime.core_count, restored.runtime.core_count);
        assert_eq!(original.raft.group_count, restored.raft.group_count);
        assert_eq!(
            original.raft.rejoin_probe.as_duration(),
            restored.raft.rejoin_probe.as_duration()
        );
        assert_eq!(
            original.storage.cold.flush_size.as_bytes(),
            restored.storage.cold.flush_size.as_bytes()
        );
        assert_eq!(
            original
                .storage
                .cold
                .cache
                .as_ref()
                .unwrap()
                .max_size
                .as_bytes(),
            restored
                .storage
                .cold
                .cache
                .as_ref()
                .unwrap()
                .max_size
                .as_bytes()
        );
    }

    #[test]
    fn load_yaml_config() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
        write!(
            tmp,
            r#"
server:
  listen: "127.0.0.1:4437"

runtime:
  core_count: 8

raft:
  group_count: 128
  init_membership: false
  wal:
    backend: "memory"
"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.server.listen, "127.0.0.1:4437");
        assert_eq!(config.runtime.core_count, 8);
        assert_eq!(config.raft.group_count, 128);
        assert_eq!(config.raft.wal.backend, WalBackend::Memory);
    }

    #[test]
    fn yaml_preset_merge() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".yml").unwrap();
        write!(
            tmp,
            r#"
server:
  listen: "0.0.0.0:4437"

runtime:
  core_count: 2
"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), Some(Preset::Standard), Some(1)).unwrap();
        // user overrides
        assert_eq!(config.server.listen, "0.0.0.0:4437");
        assert_eq!(config.runtime.core_count, 2);
        // preset still applies
        assert_eq!(config.raft.group_count, 256);
        assert_eq!(config.storage.cold.flush_size.as_bytes(), 8 * 1024 * 1024);
    }

    #[test]
    fn yaml_rejects_non_mapping_top_level() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".yaml").unwrap();
        writeln!(tmp, "42").unwrap();
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("not a mapping") || msg.contains("top-level"),
            "error should mention top-level mapping: {msg}"
        );
    }

    #[test]
    fn load_json_config() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".json").unwrap();
        write!(
            tmp,
            r#"{{
  "server": {{ "listen": "127.0.0.1:4437" }},
  "runtime": {{ "core_count": 8 }},
  "raft": {{
    "group_count": 128,
    "init_membership_per_group": false,
    "peers": [
      {{ "node_id": 1, "url": "http://10.0.0.1:4437" }},
      {{ "node_id": 2, "url": "http://10.0.0.2:4437" }}
    ],
    "wal": {{ "backend": "memory" }}
  }},
  "storage": {{
    "cold": {{ "backend": "none" }},
    "snapshot": {{ "backend": "inline" }}
  }}
}}"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.server.listen, "127.0.0.1:4437");
        assert_eq!(config.runtime.core_count, 8);
        assert_eq!(config.raft.group_count, 128);
        assert_eq!(config.raft.peers.len(), 2);
        assert_eq!(config.raft.wal.backend, WalBackend::Memory);
    }

    #[test]
    fn json_preset_merge() {
        let mut tmp = tempfile::NamedTempFile::with_suffix(".json").unwrap();
        write!(
            tmp,
            r#"{{
  "server": {{ "listen": "0.0.0.0:4437" }},
  "runtime": {{ "core_count": 2 }},
  "raft": {{
    "peers": [{{ "node_id": 1, "url": "http://10.0.0.1:4437" }}],
    "wal": {{ "backend": "memory" }}
  }},
  "storage": {{
    "cold": {{ "backend": "none" }},
    "snapshot": {{ "backend": "inline" }}
  }}
}}"#
        )
        .unwrap();
        let config = load_config(Some(tmp.path()), Some(Preset::Standard), Some(1)).unwrap();
        // user overrides
        assert_eq!(config.server.listen, "0.0.0.0:4437");
        assert_eq!(config.runtime.core_count, 2);
        // preset still applies
        assert_eq!(config.raft.group_count, 256);
        assert_eq!(config.storage.cold.flush_size.as_bytes(), 8 * 1024 * 1024);
    }

    #[test]
    fn preset_alone_without_config_file() {
        let config = load_config(None, Some(Preset::Tiny), Some(1)).unwrap();
        assert_eq!(
            config.runtime.core_count,
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        );
        assert_eq!(config.raft.group_count, 64);
        assert_eq!(config.raft.node_id, 1);
        assert_eq!(
            config
                .storage
                .cold
                .cache
                .as_ref()
                .unwrap()
                .max_size
                .as_bytes(),
            64 * 1024 * 1024
        );
    }

    #[test]
    fn preset_tiny_matches_legacy_profile() {
        let config = load_config(None, Some(Preset::Tiny), Some(1)).unwrap();
        assert_eq!(
            config.runtime.core_count,
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        );
        assert_eq!(config.runtime.live_read_max_waiters_per_core, Some(8_192));
        assert_eq!(config.raft.group_count, 64);
        assert_eq!(
            config
                .raft
                .max_uncommitted_size_per_group
                .unwrap()
                .as_bytes(),
            8 * 1024 * 1024
        );
        assert_eq!(
            config.server.http_inflight_body_size.as_bytes(),
            64 * 1024 * 1024
        );
        assert_eq!(config.storage.cold.flush_size.as_bytes(), 4 * 1024 * 1024);
        assert_eq!(config.storage.cold.flush_max_concurrency, 2);
        assert_eq!(
            config
                .storage
                .cold
                .max_hot_size_per_group
                .unwrap()
                .as_bytes(),
            8 * 1024 * 1024
        );
    }

    #[test]
    fn preset_standard_matches_legacy_profile() {
        let config = load_config(None, Some(Preset::Standard), Some(1)).unwrap();
        assert_eq!(
            config.runtime.core_count,
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        );
        assert_eq!(config.runtime.live_read_max_waiters_per_core, Some(65_536));
        assert_eq!(config.raft.group_count, 256);
        assert_eq!(
            config
                .raft
                .max_uncommitted_size_per_group
                .unwrap()
                .as_bytes(),
            64 * 1024 * 1024
        );
        assert_eq!(
            config.server.http_inflight_body_size.as_bytes(),
            256 * 1024 * 1024
        );
        assert_eq!(config.storage.cold.flush_size.as_bytes(), 8 * 1024 * 1024);
        assert_eq!(config.storage.cold.flush_max_concurrency, 4);
        assert_eq!(
            config
                .storage
                .cold
                .max_hot_size_per_group
                .unwrap()
                .as_bytes(),
            64 * 1024 * 1024
        );
    }
}
