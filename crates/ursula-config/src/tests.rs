#[cfg(test)]
mod human_tests {
    use std::time::Duration;

    use crate::human::HumanDuration;
    use crate::human::HumanSize;

    #[test]
    fn human_duration_parses_valid_inputs() {
        let cases: &[(&str, Duration)] = &[
            ("30s", Duration::from_secs(30)),
            ("250ms", Duration::from_millis(250)),
            ("2h", Duration::from_secs(7200)),
            ("1d", Duration::from_secs(86400)),
        ];
        for (input, expected) in cases {
            assert_eq!(
                input.parse::<HumanDuration>().unwrap().as_duration(),
                *expected,
                "input {input:?}"
            );
        }
    }

    #[test]
    fn human_duration_rejects_invalid_inputs() {
        for input in ["1.5ms", "30x", "-1s"] {
            assert!(
                input.parse::<HumanDuration>().is_err(),
                "input {input:?} should be rejected"
            );
        }
    }

    #[test]
    fn human_duration_from_toml_values() {
        let raw: toml::Value = toml::Value::Integer(30000);
        let dur: HumanDuration = raw.try_into().unwrap();
        assert_eq!(dur.as_duration(), Duration::from_millis(30000));

        let raw: toml::Value = toml::Value::String("5m".into());
        let dur: HumanDuration = raw.try_into().unwrap();
        assert_eq!(dur.as_duration(), Duration::from_secs(300));
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
    fn human_size_parses_valid_inputs() {
        let cases: &[(&str, u64)] = &[
            ("100B", 100),
            ("1KiB", 1024),
            ("1MiB", 1024 * 1024),
            ("1GiB", 1024 * 1024 * 1024),
            ("256MiB", 256 * 1024 * 1024),
            ("1.5GiB", 1_610_612_736),
        ];
        for (input, expected) in cases {
            assert_eq!(
                input.parse::<HumanSize>().unwrap().as_bytes(),
                *expected,
                "input {input:?}"
            );
        }
    }

    #[test]
    fn human_size_rejects_invalid_inputs() {
        for input in ["30x", "-1MiB", "99999999999999999999GiB"] {
            assert!(
                input.parse::<HumanSize>().is_err(),
                "input {input:?} should be rejected"
            );
        }
    }

    #[test]
    fn human_size_from_toml_values() {
        let raw: toml::Value = toml::Value::Integer(67108864);
        let size: HumanSize = raw.try_into().unwrap();
        assert_eq!(size.as_bytes(), 67108864);

        let raw: toml::Value = toml::Value::String("128MiB".into());
        let size: HumanSize = raw.try_into().unwrap();
        assert_eq!(size.as_bytes(), 128 * 1024 * 1024);
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
        assert_eq!(config.raft.group_count, Some(256));
        use crate::config::ColdBackend;
        use crate::config::RaftSnapshotBackend;
        assert_eq!(
            config.raft.wal.resolved_path(),
            Some(std::path::PathBuf::from("/var/lib/ursula/raft-log"))
        );
        assert_eq!(config.storage.cold.backend, ColdBackend::S3);
        assert_eq!(config.storage.snapshot.backend, RaftSnapshotBackend::S3);
        assert_eq!(config.raft.snapshot_build_max_concurrency, 2);
        assert_eq!(
            config.storage.cold.s3.as_ref().unwrap().bucket,
            Some("my-bucket".into())
        );
    }

    #[test]
    fn raft_snapshot_build_concurrency_is_configurable() {
        let config: UrsulaConfig = toml::from_str(
            r#"
[raft]
snapshot_build_max_concurrency = 4
"#,
        )
        .expect("snapshot_build_max_concurrency parses");

        assert_eq!(config.raft.snapshot_build_max_concurrency, 4);
    }

    #[test]
    fn raft_snapshot_log_retention_is_bounded_and_configurable() {
        let default = UrsulaConfig::default();
        assert_eq!(default.raft.snapshot_pressure_max_groups_per_tick, 16);
        assert_eq!(default.raft.max_in_snapshot_log_to_keep, 64);

        let config: UrsulaConfig = toml::from_str(
            r#"
[raft]
snapshot_pressure_max_groups_per_tick = 32
max_in_snapshot_log_to_keep = 128
"#,
        )
        .expect("max_in_snapshot_log_to_keep parses");

        assert_eq!(config.raft.snapshot_pressure_max_groups_per_tick, 32);
        assert_eq!(config.raft.max_in_snapshot_log_to_keep, 128);
    }

    /// #669: `raft.deleted_data_max_age` is unset by default and parses as a
    /// duration.
    #[test]
    fn raft_deleted_data_max_age_parses() {
        assert_eq!(UrsulaConfig::default().raft.deleted_data_max_age, None);
        let config: UrsulaConfig = toml::from_str(
            r#"
[raft]
deleted_data_max_age = "30m"
"#,
        )
        .expect("deleted_data_max_age parses");
        assert_eq!(
            config.effective_deleted_data_max_age(),
            std::time::Duration::from_secs(1_800)
        );
        assert_eq!(
            config.end_record_retention(),
            std::time::Duration::from_secs(900)
        );
    }

    #[test]
    fn snapshot_cadence_follows_a_node_log_budget() {
        let default = UrsulaConfig::default();
        assert_eq!(default.raft.snapshot_log_budget.as_bytes(), 1 << 30);
        assert_eq!(default.raft.snapshot_backstop_logs, 100_000);
        let config: UrsulaConfig = toml::from_str(
            r#"
[raft]
snapshot_log_budget = "256MiB"
snapshot_backstop_logs = 50000
"#,
        )
        .expect("snapshot cadence parses");
        assert_eq!(config.raft.snapshot_log_budget.as_bytes(), 256 << 20);
        assert_eq!(config.raft.snapshot_backstop_logs, 50_000);
    }

    #[test]
    fn snapshot_backend_defaults_to_s3_with_an_s3_cold_store() {
        use crate::config::ColdBackend;
        use crate::config::RaftSnapshotBackend;
        // F12b: `auto` is the default and picks S3 snapshots whenever the
        // cold store is S3; `inline` and `s3` stay explicit choices.
        assert_eq!(
            UrsulaConfig::default().storage.snapshot.backend,
            RaftSnapshotBackend::Auto
        );
        assert_eq!(
            RaftSnapshotBackend::Auto.resolve(ColdBackend::S3),
            RaftSnapshotBackend::S3
        );
        for cold in [ColdBackend::None, ColdBackend::Memory] {
            assert_eq!(
                RaftSnapshotBackend::Auto.resolve(cold),
                RaftSnapshotBackend::Inline
            );
        }
        assert_eq!(
            RaftSnapshotBackend::Inline.resolve(ColdBackend::S3),
            RaftSnapshotBackend::Inline
        );
        let explicit: UrsulaConfig = toml::from_str(
            r#"
[storage.snapshot]
backend = "inline"
"#,
        )
        .expect("explicit inline parses");
        assert_eq!(
            explicit.storage.snapshot.backend,
            RaftSnapshotBackend::Inline
        );
        let auto: UrsulaConfig = toml::from_str(
            r#"
[storage.snapshot]
backend = "auto"
"#,
        )
        .expect("auto parses");
        assert_eq!(auto.storage.snapshot.backend, RaftSnapshotBackend::Auto);
    }

    #[test]
    fn snapshot_drive_interval_is_optional_and_parses() {
        use crate::human::HumanDuration;

        let omitted: UrsulaConfig = toml::from_str(
            r#"
[storage.snapshot]
backend = "s3"
"#,
        )
        .expect("omitted drive_interval parses");
        assert_eq!(omitted.storage.snapshot.drive_interval, None);

        let explicit: UrsulaConfig = toml::from_str(
            r#"
[storage.snapshot]
backend = "s3"
drive_interval = "45s"
"#,
        )
        .expect("explicit non-zero drive_interval parses");
        assert_eq!(
            explicit.storage.snapshot.drive_interval,
            Some(HumanDuration::sec(45))
        );
    }

    #[test]
    fn cold_cache_config_has_omitted_zero_and_custom_states() {
        use crate::human::HumanSize;

        let omitted: UrsulaConfig = toml::from_str(
            r#"
[storage.cold]
backend = "memory"
"#,
        )
        .expect("omitted cache parses");
        assert_eq!(omitted.storage.cold.cache, None);

        let disabled: UrsulaConfig = toml::from_str(
            r#"
[storage.cold]
backend = "memory"

[storage.cold.cache]
max_size = "0B"
"#,
        )
        .expect("zero cache parses");
        assert_eq!(
            disabled.storage.cold.cache.unwrap().max_size,
            HumanSize::bytes(0)
        );

        let custom: UrsulaConfig = toml::from_str(
            r#"
[storage.cold]
backend = "memory"

[storage.cold.cache]
max_size = "12MiB"
block_size = "2MiB"
readahead_blocks = 7
"#,
        )
        .expect("custom cache parses");
        let cache = custom.storage.cold.cache.unwrap();
        assert_eq!(cache.max_size, HumanSize::mib(12));
        assert_eq!(cache.block_size, HumanSize::mib(2));
        assert_eq!(cache.readahead_blocks, 7);
    }
}

#[cfg(test)]
mod load_tests {
    use std::io::Write;

    use crate::load::load_config;
    use crate::preset::Preset;

    /// Write `contents` to a fresh temp file with the given suffix.
    fn temp_config(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
        let mut tmp = tempfile::NamedTempFile::with_suffix(suffix).unwrap();
        write!(tmp, "{contents}").unwrap();
        tmp
    }

    /// The snapshot driver always runs: loading a config that sets a zero
    /// `storage.snapshot.drive_interval` fails.
    #[test]
    fn a_zero_snapshot_drive_interval_is_refused_at_load() {
        let tmp = temp_config(".toml", "[storage.snapshot]\ndrive_interval = \"0s\"\n");
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            matches!(
                err,
                crate::ConfigError::Validation(
                    crate::validate::ValidationError::ZeroSnapshotDriveInterval
                )
            ),
            "{err}"
        );
    }

    /// `raft.snapshot_logs_since_last` is gone in 0.9: a config that still
    /// sets it is refused as an unknown key.
    #[test]
    fn a_removed_snapshot_logs_since_last_is_refused_at_load() {
        let tmp = temp_config(".toml", "[raft]\nsnapshot_logs_since_last = 5000\n");
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            matches!(err, crate::ConfigError::TomlParse(_))
                && err.to_string().contains("snapshot_logs_since_last"),
            "{err}"
        );
    }

    fn available_cores() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }

    #[test]
    fn load_minimal_config_without_preset() {
        let tmp = temp_config(
            ".toml",
            r#"
[server]
listen = "127.0.0.1:4437"

[runtime]
core_count = 4

[raft]
group_count = 16
"#,
        );
        let config = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.runtime.core_count, 4);
        assert_eq!(config.raft.node_id, 1);
        assert_eq!(config.raft.wal.path, None);
    }

    #[test]
    fn preset_tiny_overrides_defaults() {
        let tmp = temp_config(
            ".toml",
            r#"
[server]
listen = "127.0.0.1:4437"
"#,
        );
        let config = load_config(Some(tmp.path()), Some(Preset::Tiny), Some(1)).unwrap();
        // preset no longer overrides core_count
        assert_eq!(config.runtime.core_count, available_cores());
        assert_eq!(config.raft.group_count, Some(64)); // from tiny preset
        assert_eq!(config.raft.wal.path, None); // a single node may run without a path
    }

    /// The tiny and small presets give a cold-backed node the smallest
    /// in-flight body budget that holds a request and a snapshot commit's
    /// copy; a smaller one is refused at load.
    #[test]
    fn a_cold_backed_node_needs_room_for_a_request_and_a_copy() {
        for preset in [Preset::Tiny, Preset::Small] {
            let tmp = temp_config(
                ".toml",
                r#"
[server]
listen = "127.0.0.1:4437"

[storage.cold]
backend = "memory"
"#,
            );
            let config = load_config(Some(tmp.path()), Some(preset), Some(1)).unwrap();
            assert_eq!(
                config.server.http_inflight_body_size.as_bytes(),
                crate::config::ServerConfig::MIN_INFLIGHT_BODY_SIZE_WITH_COLD,
                "{preset:?}"
            );
        }
        let tmp = temp_config(
            ".toml",
            r#"
[server]
listen = "127.0.0.1:4437"
http_inflight_body_size = "63MiB"

[storage.cold]
backend = "memory"
"#,
        );
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            matches!(
                err,
                crate::load::ConfigError::Validation(
                    crate::validate::ValidationError::InflightBodyBudgetTooSmall { .. }
                )
            ),
            "{err}"
        );
        // Without a cold backend nothing copies, and the budget may be smaller.
        let tmp = temp_config(
            ".toml",
            r#"
[server]
listen = "127.0.0.1:4437"
http_inflight_body_size = "33MiB"
"#,
        );
        load_config(Some(tmp.path()), None, Some(1)).unwrap();
    }

    #[test]
    fn user_config_overrides_preset() {
        let tmp = temp_config(
            ".toml",
            r#"
[runtime]
core_count = 2
"#,
        );
        let config = load_config(Some(tmp.path()), Some(Preset::Tiny), Some(1)).unwrap();
        assert_eq!(config.runtime.core_count, 2); // user overrides preset's 4
        assert_eq!(config.raft.group_count, Some(64)); // preset still applies
    }

    #[test]
    fn validation_requires_a_wal_path_with_peers() {
        let peers = r#"
[[raft.peers]]
node_id = 1
url = "http://127.0.0.1:4437"

[[raft.peers]]
node_id = 2
url = "http://127.0.0.1:4438"
"#;
        let tmp = temp_config(".toml", peers);
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            matches!(
                err,
                crate::ConfigError::Validation(
                    crate::validate::ValidationError::RaftWalPathRequired
                )
            ),
            "{err}"
        );

        let tmp = temp_config(
            ".toml",
            &format!("[raft.wal]\npath = \"/tmp/ursula-wal\"\n{peers}"),
        );
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            matches!(
                err,
                crate::ConfigError::Validation(crate::validate::ValidationError::ClusterIdRequired)
            ),
            "{err}"
        );

        let tmp = temp_config(
            ".toml",
            &format!(
                "[server]\ncluster_id = \"prod-eu-1\"\n[raft]\ngroup_count = 8\n[raft.wal]\npath = \"/tmp/ursula-wal\"\n{peers}"
            ),
        );
        let config = load_config(Some(tmp.path()), None, Some(1))
            .expect("a static cluster with a WAL path and a cluster id");
        assert_eq!(
            config.server.cluster_id.as_ref().map(|id| id.as_str()),
            Some("prod-eu-1")
        );
    }

    #[test]
    fn a_split_cluster_plane_needs_every_peers_client_url() {
        let config = |client_url_2: &str| {
            format!(
                r#"[server]
listen = "0.0.0.0:4437"
cluster_listen = "0.0.0.0:4439"
cluster_id = "prod-eu-1"

[raft]
group_count = 8

[raft.wal]
path = "/tmp/ursula-wal"

[[raft.peers]]
node_id = 1
url = "http://node-1:4439"
client_url = "http://node-1:4437"

[[raft.peers]]
node_id = 2
url = "http://node-2:4439"
{client_url_2}
"#
            )
        };
        let tmp = temp_config(".toml", &config(""));
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            matches!(
                err,
                crate::ConfigError::Validation(
                    crate::validate::ValidationError::PeerClientUrlRequired { node_id: 2 }
                )
            ),
            "{err}"
        );

        let tmp = temp_config(".toml", &config(r#"client_url = "http://node-2:4439/""#));
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            matches!(
                err,
                crate::ConfigError::Validation(
                    crate::validate::ValidationError::PeerClientUrlIsRaftUrl { node_id: 2 }
                )
            ),
            "{err}"
        );

        let tmp = temp_config(".toml", &config(r#"client_url = "http://node-2:4437""#));
        let config = load_config(Some(tmp.path()), None, Some(1))
            .expect("a split cluster plane with every peer's client URL");
        let bases: Vec<_> = config
            .raft
            .peers
            .iter()
            .map(|peer| (peer.url.as_str(), peer.client_base_url()))
            .collect();
        assert_eq!(bases, [
            ("http://node-1:4439", "http://node-1:4437"),
            ("http://node-2:4439", "http://node-2:4437"),
        ]);
    }

    #[test]
    fn a_cluster_id_outside_the_grammar_fails_the_load() {
        for id in ["", "a/b", "a b", &"x".repeat(65)] {
            let tmp = temp_config(".toml", &format!("[server]\ncluster_id = \"{id}\"\n"));
            let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
            assert!(
                matches!(&err, crate::ConfigError::TomlParse(parse)
                    if parse.message().contains("ursula-cluster-id")),
                "{id}: {err}"
            );
        }
        let tmp = temp_config(".toml", "[server]\ncluster_id = \"u0123.dev_A-1\"\n");
        let config = load_config(Some(tmp.path()), None, Some(1)).expect("a valid id");
        let rendered = toml::to_string(&config).expect("serialize");
        assert!(
            rendered.contains("cluster_id = \"u0123.dev_A-1\""),
            "{rendered}"
        );
    }

    /// `raft.meta_voters` and `raft.placement` parse into the kernel's
    /// types: unset they default (no meta voters, RF 3, 4 moves), a repeated
    /// voter counts once and a zero move limit is kept.
    #[test]
    fn meta_voters_and_placement_parse_into_the_kernel_types() {
        use ursula_proto::placement::NodeId;

        let tmp = temp_config(".toml", "");
        let defaults = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(defaults.raft.meta_voters, None);
        assert_eq!(defaults.raft.placement.replication_factor.get(), 3);
        assert_eq!(defaults.raft.placement.max_concurrent_moves.get(), 4);

        let tmp = temp_config(
            ".toml",
            r#"
[server]
cluster_id = "meta-test"

[raft]
meta_voters = [3, 1, 3]
group_count = 8

[raft.wal]
path = "/tmp/ursula-wal"

[raft.placement]
replication_factor = 2
max_concurrent_moves = 0

[[raft.peers]]
node_id = 1
url = "http://127.0.0.1:4437"

[[raft.peers]]
node_id = 3
url = "http://127.0.0.1:4438"
"#,
        );
        let config = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        let voters = config.raft.meta_voters.expect("meta voters are set");
        assert_eq!(voters.iter().collect::<Vec<_>>(), vec![
            NodeId::new(1),
            NodeId::new(3)
        ]);
        assert_eq!(config.raft.placement.replication_factor.get(), 2);
        assert_eq!(config.raft.placement.max_concurrent_moves.get(), 0);
    }

    /// An empty meta voter list and a zero replication factor fail the
    /// parse, and so does an unknown `raft.placement` key.
    #[test]
    fn an_empty_meta_voter_list_and_a_zero_replication_factor_are_refused() {
        for (contents, key) in [
            ("[raft]\nmeta_voters = []\n", "meta_voters"),
            (
                "[raft.placement]\nreplication_factor = 0\n",
                "replication_factor",
            ),
            ("[raft.placement]\ngroup_voters = 3\n", "group_voters"),
        ] {
            let tmp = temp_config(".toml", contents);
            let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
            assert!(
                matches!(err, crate::ConfigError::TomlParse(_)) && err.to_string().contains(key),
                "{contents}: {err}"
            );
        }
    }

    /// The meta voters are nodes of `raft.peers`, or, with it empty (a node
    /// on a temporary WAL), this node alone.
    #[test]
    fn meta_voters_outside_the_peers_are_refused() {
        let refused = |contents: &str| {
            let tmp = temp_config(".toml", contents);
            match load_config(Some(tmp.path()), None, Some(1)) {
                Err(crate::ConfigError::Validation(
                    crate::validate::ValidationError::MetaVoterOutsidePeers { node_id },
                )) => Some(node_id),
                Err(err) => panic!("{contents}: {err}"),
                Ok(_) => None,
            }
        };
        assert_eq!(refused("[raft]\nmeta_voters = [1]\n"), None);
        assert_eq!(refused("[raft]\nmeta_voters = [1, 2]\n"), Some(2));
        let cluster = r#"
[server]
cluster_id = "meta-test"

[raft.wal]
path = "/tmp/ursula-wal"

[[raft.peers]]
node_id = 1
url = "http://127.0.0.1:4437"

[[raft.peers]]
node_id = 2
url = "http://127.0.0.1:4438"
"#;
        assert_eq!(
            refused(&format!(
                "[raft]\nmeta_voters = [2, 1]\ngroup_count = 8\n{cluster}"
            )),
            None
        );
        assert_eq!(
            refused(&format!(
                "[raft]\nmeta_voters = [1, 4]\ngroup_count = 8\n{cluster}"
            )),
            Some(4)
        );
    }

    /// The memory WAL is gone: its settings are unknown keys, which fail the
    /// load instead of being ignored.
    #[test]
    fn removed_memory_wal_settings_are_refused() {
        for (section, key) in [
            ("raft.wal", "backend = \"memory\""),
            ("raft.wal", "backend = \"disk\""),
            ("raft.wal", "allow_volatile_multi_peer = true"),
            ("raft", "memory_bootstrap_marker_dir = \"/tmp/markers\""),
        ] {
            let tmp = temp_config(".toml", &format!("[{section}]\n{key}\n"));
            let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
            assert!(
                matches!(err, crate::ConfigError::TomlParse(_)),
                "{key}: {err}"
            );
            let name = key.split(' ').next().unwrap();
            assert!(
                err.to_string().contains(&format!("unknown field `{name}`")),
                "{key}: {err}"
            );
        }
    }

    /// Static voter assignment is gone: data groups take the seed rule's
    /// voters and initializer, so the keys that set them fail the load.
    #[test]
    fn removed_static_membership_keys_are_refused() {
        for (key, contents) in [
            ("init_membership", "[raft]\ninit_membership = true\n"),
            (
                "init_membership_per_group",
                "[raft]\ninit_membership_per_group = true\n",
            ),
            (
                "groups",
                "[[raft.groups]]\nraft_group_id = 0\nvoters = [1]\n",
            ),
        ] {
            let tmp = temp_config(".toml", contents);
            let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
            assert!(
                matches!(err, crate::ConfigError::TomlParse(_)),
                "{key}: {err}"
            );
            assert!(
                err.to_string().contains(&format!("unknown field `{key}`")),
                "{key}: {err}"
            );
        }
    }

    #[test]
    fn wal_fsync_is_unset_by_default_and_parses_both_policies() {
        use crate::config::WalFsync;

        assert_eq!(crate::UrsulaConfig::default().raft.wal.fsync, None);
        let disk = |fsync: &str| format!("[raft.wal]\n{fsync}\n");
        for (line, expected) in [
            ("", None),
            ("fsync = \"always\"", Some(WalFsync::Always)),
            ("fsync = \"never\"", Some(WalFsync::Never)),
        ] {
            let tmp = temp_config(".toml", &disk(line));
            let config = load_config(Some(tmp.path()), None, Some(1)).expect("valid fsync policy");
            assert_eq!(config.raft.wal.fsync, expected, "{line:?}");
        }

        for invalid in ["fsync = \"interval\"", "fsync = \"Always\"", "fsync = true"] {
            let tmp = temp_config(".toml", &disk(invalid));
            let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
            assert!(
                matches!(err, crate::ConfigError::TomlParse(_)),
                "{invalid:?}: {err}"
            );
        }
    }

    #[test]
    fn validation_rejects_disk_pressure_resume_at_or_below_minimum() {
        let tmp = temp_config(
            ".toml",
            r#"
[raft.wal]
min_available_size = "1GiB"
resume_available_size = "512MiB"
"#,
        );
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            err.to_string().contains("resume_available_size"),
            "error should name the invalid watermark: {err}"
        );
    }

    #[test]
    fn wal_segment_and_cache_sizes_load_and_split_the_cache_between_groups() {
        let config = load_config(None, None, Some(1)).unwrap();
        assert_eq!(config.raft.wal.segment_size.as_bytes(), 64 << 20);
        assert_eq!(config.raft.wal.cache_size.as_bytes(), 256 << 20);
        assert_eq!(config.raft.wal.group_cache_bytes(128), 2 << 20);
        assert_eq!(config.raft.wal.group_cache_bytes(0), 256 << 20);

        let tmp = temp_config(
            ".toml",
            r#"
[raft.wal]
segment_size = "8MiB"
cache_size = "32MiB"
"#,
        );
        let config = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.raft.wal.segment_size.as_bytes(), 8 << 20);
        assert_eq!(config.raft.wal.group_cache_bytes(32), 1 << 20);

        let tmp = temp_config(
            ".toml",
            r#"
[raft.wal]
segment_size = "1KiB"
"#,
        );
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        assert!(
            matches!(
                err,
                crate::ConfigError::Validation(
                    crate::validate::ValidationError::WalSegmentTooSmall {
                        segment_size: 1024,
                        minimum: 4096
                    }
                )
            ),
            "{err}"
        );
    }

    #[test]
    fn validation_rejects_s3_without_bucket() {
        let tmp = temp_config(
            ".toml",
            r#"
[storage.cold]
backend = "s3"
"#,
        );
        let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("bucket"), "error should mention bucket: {msg}");
    }

    #[test]
    fn nested_table_merge() {
        let tmp = temp_config(
            ".toml",
            r#"
[storage.cold.cache]
max_size = "128MiB"
"#,
        );
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
        let tmp = temp_config(
            ".toml",
            r#"
[server]
cluster_id = "test"

[raft]
group_count = 8

[raft.wal]
path = "/tmp/ursula-wal"

[[raft.peers]]
node_id = 1
url = "http://10.0.0.1:4437"

[[raft.peers]]
node_id = 2
url = "http://10.0.0.2:4437"
"#,
        );
        let config = load_config(Some(tmp.path()), None, Some(1)).unwrap();
        assert_eq!(config.raft.peers.len(), 2);
        assert_eq!(config.raft.peers[0].node_id, 1);
        assert_eq!(config.raft.peers[1].node_id, 2);
    }

    #[test]
    fn node_id_from_cli_overrides_file() {
        let tmp = temp_config(
            ".toml",
            r#"
[raft]
node_id = 1
"#,
        );
        // CLI --node-id 42 overrides file's node_id = 1
        let config = load_config(Some(tmp.path()), None, Some(42)).unwrap();
        assert_eq!(config.raft.node_id, 42);
    }

    #[test]
    fn validation_rejects_missing_node_id() {
        let tmp = temp_config(
            ".toml",
            r#"
[server]
listen = "127.0.0.1:4437"
"#,
        );
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
            original.raft.bootstrap_peer_probe.as_duration(),
            restored.raft.bootstrap_peer_probe.as_duration()
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
    fn preset_alone_without_config_file() {
        let config = load_config(None, Some(Preset::Tiny), Some(1)).unwrap();
        assert_eq!(config.runtime.core_count, available_cores());
        assert_eq!(config.raft.group_count, Some(64));
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
    fn presets_match_legacy_profiles() {
        let mib = |n: u64| n * 1024 * 1024;
        // (preset, name, (live_read_waiters, group_count, uncommitted_bytes,
        //  http_inflight_bytes, flush_bytes, flush_concurrency, hot_bytes))
        let cases = [
            (
                Preset::Tiny,
                "tiny",
                (Some(8_192), 64, mib(8), mib(64), mib(4), 2, mib(8)),
            ),
            (
                Preset::Standard,
                "standard",
                (Some(65_536), 256, mib(64), mib(256), mib(8), 4, mib(64)),
            ),
        ];
        for (preset, name, expected) in cases {
            let config = load_config(None, Some(preset), Some(1)).unwrap();
            assert_eq!(
                config.runtime.core_count,
                available_cores(),
                "preset {name}: core_count"
            );
            let actual = (
                config.runtime.live_read_max_waiters_per_core,
                config.raft.effective_group_count(),
                config
                    .raft
                    .max_uncommitted_size_per_group
                    .unwrap()
                    .as_bytes(),
                config.server.http_inflight_body_size.as_bytes(),
                config.storage.cold.flush_size.as_bytes(),
                config.storage.cold.flush_max_concurrency,
                config
                    .storage
                    .cold
                    .max_hot_size_per_group
                    .unwrap()
                    .as_bytes(),
            );
            // Tuple order: (live_read_waiters, group_count, uncommitted,
            // http_inflight, flush_size, flush_concurrency, max_hot_size).
            assert_eq!(actual, expected, "preset {name}");
        }
    }
    #[test]
    fn s3_server_side_encryption_defaults_to_aes256() {
        use crate::config::S3ServerSideEncryption;
        use crate::config::UrsulaConfig;
        let config: UrsulaConfig = toml::from_str(
            r#"
[storage.cold]
backend = "s3"

[storage.cold.s3]
bucket = "my-bucket"
"#,
        )
        .expect("valid config");
        let s3 = config.storage.cold.s3.expect("s3 config");
        assert_eq!(s3.server_side_encryption, S3ServerSideEncryption::Aes256);
        assert_eq!(s3.kms_key_id, None);
    }

    #[test]
    fn s3_server_side_encryption_parses_all_modes() {
        use crate::config::S3ServerSideEncryption;
        use crate::config::UrsulaConfig;
        for (value, expected) in [
            ("aes256", S3ServerSideEncryption::Aes256),
            ("aws-kms", S3ServerSideEncryption::AwsKms),
            ("none", S3ServerSideEncryption::None),
        ] {
            let toml = format!(
                r#"
[storage.cold]
backend = "s3"

[storage.cold.s3]
bucket = "my-bucket"
server_side_encryption = "{value}"
"#
            );
            let config: UrsulaConfig = toml::from_str(&toml).expect("valid config");
            let s3 = config.storage.cold.s3.expect("s3 config");
            assert_eq!(s3.server_side_encryption, expected, "mode {value}");
        }
    }

    #[test]
    fn s3_kms_key_id_round_trips() {
        use crate::config::S3ServerSideEncryption;
        use crate::config::UrsulaConfig;
        let config: UrsulaConfig = toml::from_str(
            r#"
[storage.cold]
backend = "s3"

[storage.cold.s3]
bucket = "my-bucket"
server_side_encryption = "aws-kms"
kms_key_id = "arn:aws:kms:us-east-1:111122223333:key/test"
"#,
        )
        .expect("valid config");
        let s3 = config.storage.cold.s3.expect("s3 config");
        assert_eq!(s3.server_side_encryption, S3ServerSideEncryption::AwsKms);
        assert_eq!(
            s3.kms_key_id.as_deref(),
            Some("arn:aws:kms:us-east-1:111122223333:key/test")
        );
    }

    #[test]
    fn s3_health_gate_rides_out_one_failed_probe_by_default_and_keeps_explicit_keys() {
        use std::time::Duration;

        use crate::config::UrsulaConfig;
        let defaults: UrsulaConfig = toml::from_str(
            r#"
[storage.cold]
backend = "s3"

[storage.cold.s3]
bucket = "my-bucket"
"#,
        )
        .expect("valid config");
        let s3 = defaults.storage.cold.s3.expect("s3 config");
        assert_eq!(s3.probe_timeout.as_duration(), Duration::from_secs(5));
        assert_eq!(s3.unhealthy_ticks, 3);
        assert_eq!(s3.heal_ticks, 2);

        // A config written for the earlier defaults keeps them.
        let explicit: UrsulaConfig = toml::from_str(
            r#"
[storage.cold]
backend = "s3"

[storage.cold.s3]
bucket = "my-bucket"
probe_timeout = "2s"
unhealthy_ticks = 1
heal_ticks = 4
"#,
        )
        .expect("valid config");
        let s3 = explicit.storage.cold.s3.expect("s3 config");
        assert_eq!(s3.probe_timeout.as_duration(), Duration::from_secs(2));
        assert_eq!(s3.unhealthy_ticks, 1);
        assert_eq!(s3.heal_ticks, 4);
    }

    /// #561: no build could run with 0, which was taken as 1 without a word,
    /// and a value past the bound is taken for a mistake.
    #[test]
    fn a_snapshot_build_concurrency_out_of_bounds_fails_to_load() {
        use crate::config::RaftConfig;
        use crate::validate::ValidationError;

        let max = RaftConfig::MAX_SNAPSHOT_BUILD_CONCURRENCY;
        for (value, valid) in [(0, false), (1, true), (max, true), (max + 1, false)] {
            let tmp = temp_config(
                ".toml",
                &format!("[raft]\nsnapshot_build_max_concurrency = {value}\n"),
            );
            let loaded = load_config(Some(tmp.path()), None, Some(1));
            if valid {
                let config = loaded.unwrap();
                assert_eq!(config.raft.snapshot_build_max_concurrency, value);
            } else {
                let err = loaded.unwrap_err();
                assert!(
                    matches!(
                        err,
                        crate::ConfigError::Validation(
                            ValidationError::SnapshotBuildConcurrency { value: refused }
                        ) if refused == value
                    ),
                    "{value}: {err}"
                );
            }
        }
    }

    #[test]
    fn a_zero_s3_health_gate_setting_fails_to_load() {
        use crate::validate::S3HealthSetting;
        use crate::validate::ValidationError;

        for (key, value, expected) in [
            ("probe_timeout", r#""0s""#, S3HealthSetting::ProbeTimeout),
            ("unhealthy_ticks", "0", S3HealthSetting::UnhealthyTicks),
            ("heal_ticks", "0", S3HealthSetting::HealTicks),
        ] {
            let tmp = temp_config(
                ".toml",
                &format!(
                    r#"
[storage.cold]
backend = "s3"

[storage.cold.s3]
bucket = "my-bucket"
{key} = {value}
"#
                ),
            );
            let err = load_config(Some(tmp.path()), None, Some(1)).unwrap_err();
            assert!(
                matches!(
                    err,
                    crate::ConfigError::Validation(ValidationError::ZeroS3HealthSetting {
                        setting
                    }) if setting == expected
                ),
                "{key}: {err}"
            );
        }
    }
}

mod snapshot_prefix_tests {
    use crate::RaftSnapshotConfig;
    use crate::SnapshotPrefix;
    use crate::SnapshotPrefixError;
    use crate::SnapshotPrefixProblem;

    fn problem(prefix: &str) -> SnapshotPrefixProblem {
        SnapshotPrefix::try_from(prefix).expect_err(prefix).problem
    }

    /// A prefix is trimmed of a leading and trailing `/`, and its components
    /// are kept as written.
    #[test]
    fn a_snapshot_prefix_keeps_its_components() {
        for (configured, kept) in [
            ("snapshots", "snapshots"),
            ("snapshots/", "snapshots"),
            ("/snapshots/", "snapshots"),
            ("a/b/", "a/b"),
            ("team-1/snap_shots.v2", "team-1/snap_shots.v2"),
        ] {
            assert_eq!(SnapshotPrefix::try_from(configured).unwrap().as_str(), kept);
        }
        assert_eq!(SnapshotPrefix::default().as_str(), "snapshots");
    }

    /// Every prefix an object store would rewrite is refused: empty, an
    /// empty or dot component, whitespace, which OpenDAL trims, and any
    /// other character outside the safe set.
    #[test]
    fn a_snapshot_prefix_refuses_what_a_store_rewrites() {
        for empty in ["", "/", "//"] {
            assert_eq!(problem(empty), SnapshotPrefixProblem::Empty, "{empty:?}");
        }
        assert_eq!(problem("a//b"), SnapshotPrefixProblem::EmptyComponent);
        assert_eq!(problem("a/./b"), SnapshotPrefixProblem::DotComponent);
        assert_eq!(problem(".."), SnapshotPrefixProblem::DotComponent);
        for (prefix, found) in [
            (" snapshots", ' '),
            (" /snapshots", ' '),
            ("snapshots ", ' '),
            ("snap\tshots", '\t'),
            ("snäpshots", 'ä'),
            ("snap*", '*'),
        ] {
            assert_eq!(
                problem(prefix),
                SnapshotPrefixProblem::Character { found },
                "{prefix:?}"
            );
        }
    }

    /// The config refuses a bad prefix where it is read, and the error
    /// names the key.
    #[test]
    fn the_config_refuses_a_bad_prefix_when_read() {
        let parsed: RaftSnapshotConfig =
            toml::from_str("backend = \"s3\"\ns3_prefix = \"/team/snapshots/\"\n").unwrap();
        assert_eq!(parsed.snapshot_prefix().as_str(), "team/snapshots");
        assert_eq!(
            RaftSnapshotConfig::default().snapshot_prefix().as_str(),
            "snapshots"
        );
        let refused = toml::from_str::<RaftSnapshotConfig>("s3_prefix = \" snapshots\"\n")
            .expect_err("a leading space");
        assert!(
            refused.to_string().contains("storage.snapshot.s3_prefix"),
            "{refused}"
        );
        let error = SnapshotPrefixError {
            prefix: String::new(),
            problem: SnapshotPrefixProblem::Empty,
        };
        assert_eq!(SnapshotPrefix::try_from(""), Err(error));
    }
}
